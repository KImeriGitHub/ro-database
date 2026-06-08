# AssetData design choices

Extracted from [SPEC.md](SPEC.md) for downstream consumers (feature
generation lives in a separate repo and has no access to this codebase).
These are the contracts the transformation step honours, and the
contracts feature generation must honour in turn.

For exact column types and names, see
[AssetData_specifications.md](AssetData_specifications.md). This document
covers semantics, not schema.

## 1. Storage and lifecycle

- **One `AssetData` instance per `(asset_type, symbol)`**, persisted as
  `<dest>/<asset_type>/data_<SYMBOL>/` (one folder per symbol, one parquet
  per frame, plus `metadata.json`). `<SYMBOL>` on disk is the
  `fs_symbol`-encoded form of the canonical AV ticker, so slash-class
  tickers like `BC/PB` materialise as a single directory
  (`data_BC%2FPB/`) instead of splitting the path. The encoding is
  reversible; in-memory `metadata.json:ticker` always carries the
  canonical form.

## 2. assets_overview.parquet (the index)

- Single index table at the destination root covering every symbol across
  every asset type. Columns: `symbol`, `assetType`, `about`,
  `reportedDate`, `timeOfTheDay`, `sector`.
- **Every symbol from every asset catalog appears exactly once.**
- Missing-value convention: `""` for Utf8 columns (`about`,
  `timeOfTheDay`, `sector`), `null` for `reportedDate` (Date columns can't
  hold an empty string).
- **`sector` is verbatim from the catalog**; canonicalization (mapping to
  `CANONICAL_SECTORS`, falling through to `Other`) happens downstream in
  `AssetData.py`'s `sector_to_index`. Non-canonical strings here silently
  degrade to `Other` later.

## 3. Schema discipline

- **Every per-frame builder ends with an explicit cast against
  `SCHEMAS[name]`.** Any column whose dtype or name does not match the
  registered schema is a **fatal error** for that symbol: the cast
  raises, the symbol's transformation aborts, and a row is logged in
  `transformation_report.parquet`.
- We **do not silently coerce, drop, or re-order** columns. Schema drift
  surfaces immediately rather than being papered over.
- Consequence for feature generation: the schemas in
  `AssetData_specifications.md` are load-bearing contracts. If a frame
  loads, its columns and dtypes are exactly as specified.

## 4. Deduplication

- All frames that concatenate snapshots over time
  (`shareprice_daily`, `shareprice_intraday`, `price_daily`,
  `etf_profile`, `insider_df`, `sentiment_df`) are deduplicated on a
  per-frame key. **Daily snapshots win over historical** when they
  collide.
- Dedup keys per frame:
  - `shareprice_daily`, `price_daily`, `etf_profile`: `Date`
  - `shareprice_intraday`: `Datetime`
  - `insider_df`: `(transactionDate, executive, security_type)`
  - `sentiment_df`: `(Datetime, url)`
- **Output is sorted ascending by the dedup key.** The dedup helper
  (`frames/_dedup.py`) sorts by `(key..., _source_order)` before
  collapsing duplicates, then re-sorts the surviving rows by `key`
  alone before returning. Consumers can rely on every deduped frame
  arriving in key-ascending order without an explicit sort downstream.

## 5. Null and drop policies (asymmetric by frame)

The drop policy is deliberately different per frame; consumers should not
assume any uniform "no nulls" guarantee.

| Frame | Null/drop policy |
|---|---|
| `shareprice_daily` | **No nulls.** Any row with a null Float32 column after dedup is dropped (logged via `dedup_dropped_null_row`). |
| `shareprice_intraday` | **Nulls preserved.** Null counts/ratios logged via `intraday_null_field`. Imputation is the consumer's job. |
| `price_daily` | Drop on null `Open`/`High`/`Low`/`Close`. **`Volume` nulls are kept** (forex / indices / commodities have no Volume in source; cryptocurrencies are the only flat asset type with real Volume). |
| `etf_profile` | Sparse in time by design. No null-driven drops. |
| `insider_df` | Drop rows with null `Shares` or invalid `AcqDis`. |
| `sentiment_df` | No null-driven drops. |
| `financials_*` | Defensive nulling on out-of-range positions and >10-day fiscalDateEnding mismatches. |

`shareprice_intraday` may contain rows with null OHLCV values, gaps in
time coverage, and uneven bar density. Imputing missing values, removing
rows around big gaps, and otherwise cleaning the time grid is the
**responsibility of feature generation**, not transformation. Nulls are
surfaced via `transformation_report.parquet` but not mutated.

## 6. Adjusted prices: single-day `AdjFactor`

The frames carry **raw, unadjusted OHLCV** in both `shareprice_daily`
and `shareprice_intraday`. Adjusted prices are not stored. Instead,
`shareprice_daily` exposes a single-day multiplier `AdjFactor`
that consumers can fold into adjusted series on demand.

### Definition

For each row `i` of `shareprice_daily` (sorted ascending by `Date`),

```
AdjFactor[i] = SplitCoefficient[i] * Close[i-1] / (Close[i-1] - DividendAmount[i])     for i >= 1
AdjFactor[0] = 1.0
```

Conventions:

- `SplitCoefficient[i]` is the split coefficient on day `i` as
  reported by the source (`1.0` on non-split days, `2.0` on a 2-for-1
  ex-split day, etc.).
- `DividendAmount[i]` is the dividend paid on day `i` (the ex-date),
  in the same currency / split units as `Close[i]` (USD, post-split).
  `0.0` on non-dividend days.
- `Close[i-1]` is the previous row's close. The first row has no
  prior close to anchor on, so `AdjFactor[0]` is fixed at `1.0`.
- On a row with no split and no dividend, `AdjFactor[i] = 1.0`
  exactly (`SplitCoefficient = 1`, `DividendAmount = 0`).

The factor is constructed so that

```
Close[i] * AdjFactor[i] / Close[i-1]  -  1   ≈   gross total return on day i
```

(splits and dividends absorbed). It is the CRSP / Yahoo "adjustment
factor" anchored on the previous close.

### What this is NOT

- **Not cumulative.** `AdjFactor` is a per-day multiplier;
  reconstructing an `AdjClose` series requires a cumulative product
  done downstream. This deliberate split keeps lookahead bias out of
  the stored frame: any future-looking adjustment is the consumer's
  decision, not a property of the dataset.
- **Not provided on the intraday frame.** `shareprice_intraday` holds
  raw OHLCV only. Consumers that want adjusted intraday returns
  should join `shareprice_daily.AdjFactor` on the calendar date of
  `shareprice_intraday.Datetime`.

### Lookahead-bias implications

Because `AdjFactor[i]` depends only on row `i`'s
`SplitCoefficient` / `DividendAmount` and the **prior** row's
`Close`, it carries no future information. A consumer that builds a
PIT-correct adjusted close as

```
AdjClose_PIT[i; as_of=t] = Close[i] * prod_{k = i+1..t} AdjFactor[k]
```

introduces lookahead only as far as `t` (the as-of date), which is
the choice the consumer is already making. The Yahoo / CRSP-style
"divide all history by the latest cumulative factor" series is one
such choice (with `t = last row`); other consumers can pick a
different `t`.

## 7. Intraday orphan rule

`shareprice_intraday` rows whose calendar date is not present in
`shareprice_daily.Date` for the same symbol are **dropped**. 
The opposite case (a daily Date with no matching intraday) is normal. 
Consumers can rely on:
**every intraday row's calendar date appears in `shareprice_daily`.**

## 8. Insider data semantics

- Output is a **chronological list of transactions, not a per-trading-date
  snapshot.** Avoiding lookahead leakage when projecting insider activity
  forward (e.g. computing rolling buy/sell pressure) is the
  responsibility of feature generation.
- Only modelling-relevant columns are kept: `Date`, `Executive_role`,
  `AcqDis`, `Shares`. `share_price` is dropped from the schema.
- `Shares` is a **raw count, not a USD amount**.
- `Executive_role` is mapped from `executive_title` via an ordered
  case-insensitive substring rule list (most-specific first; first match
  wins; empty/null/unmatched fall through to `"Other"`). The rule order
  is the spec — see `_INSIDER_ROLE_RULES` in `frames/insider.py`.
- `AcqDis` is verbatim `"A"` / `"D"` only; rows with any other value (or
  null) are dropped.
- **Retroactive amendments are not modelled.** Dedup keeps the most
  recent source row per
  `(transactionDate, executive, security_type)`, so a late amendment
  that changes only `shares` or `share_price` overwrites silently.
  Raw `daily/*/stocks/insider/`
  retains every snapshot for offline PIT replay if needed.

## 9. Sentiment data semantics

- Only **numeric scores plus `Datetime`** are kept. Titles, urls,
  authors, summaries, banner images, source labels, and sentiment string
  labels are dropped at the cast step.
- Defensive `ticker == symbol` filter when the column is present (the
  per-symbol files already filter upstream, but this is not guaranteed).
- Dedup key `(Datetime, url)`: two articles published in the same minute
  with different urls survive as distinct rows.
- **The most recent ~hour-plus of `sentiment_df` is incomplete.** AV's news
  feed lags and backfills: measured leading-edge freshness is ~36 min
  median (p90 ~38 min), with older articles landing later still. Each
  daily pull's tail is therefore under-populated; the 7-day overlap on
  later runs fills it, but the dataset's trailing edge always lags. Treat a
  sparse last hour (conservatively more) as "feed not yet complete", not
  "no news". Historical rows past the overlap window are unaffected.

## 10. Financials: per-row PIT semantics

This is the most subtle part of the contract. Read this section before
building any feature on `financials_quarterly` / `financials_annually`.

### Row axis

The row axis of `financials_quarterly` and `financials_annually` is
`shareprice_daily.Date`. **A stock with no prices produces empty
financials frames.**

### PIT correctness

Each row at date `d` reflects the financials view as known at `d`.
Restatements filed after `d` do **not** leak backward into
pre-restatement rows; retroactive amendments are visible only on rows
whose `d` is on or after the amendment.

### Historical baseline carries restatements

PIT correctness only protects rows whose `d` falls inside the
**daily-snapshot** era. Earlier rows fall back to the historical bulk
download, which the source had already restated by the time we
fetched it. As a result:

- **Every financials cell at a row date `d` older than the first
  daily snapshot may reflect post-`d` restatements** (the source's
  best-known number at the historical run date, not what was actually
  filed at `d`). These rows look PIT-correct in the schema but are
  not provider-PIT.
- Once daily snapshots have accumulated, rows whose `d` falls in the
  daily era are protected: the per-row snapshot resolution picks up
  the file as it stood at `d`, and any later amendment shows up only
  on `d' >= amendment_date`.

**Identifying the boundary.** There is no dedicated "PIT start" field
in the dataset. A practical proxy is the **earliest `Date` in
`etf_profile`**: the historical bulk contributes a single row dated
to the historical run's data-complete date, and daily folders
contribute one row per snapshot. So
`min(etf_profile.Date)` is the historical run's data-complete date,
and rows in `financials_*` with `d < min(etf_profile.Date)` should be
treated as historical-baseline (restatements possibly baked in)
rather than truly PIT. This proxy is only available on etfs; for a
universe-wide marker, take the smallest `min(etf_profile.Date)`
across the etf catalog. Equivalently, the smallest daily-folder date
in the raw layer is the same boundary, but consumers in the next
repo do not have access to that layer and must rely on
`etf_profile`.

### m-axis ordering by reportedDate

- The m-axis is ordered by **`reportedDate`** — m-position represents
  "what had been filed when", not fiscal-period order.
- **Late filers are PIT-correct, not anomalies.** A quarter whose
  `reportedDate` post-dates a later quarter's will appear with a
  `fiscalDateEnding` out of sequence relative to neighbours.
- `reportTime` is normalised to `{pre-market, post-market, other}`.
  Empty strings, nulls, and unknown labels map to `other`.

### m-anchor and the no-anchor defensive null

- `m = 0` anchors on the next quarterly report not yet filed at `d`
  (the smallest-`reportedDate` position with `reportedDate > d`).
  `m = 1` is the most recent already-filed report, and so on.
- **If no upcoming report can be identified, every `_qm{m>=0}` and every
  `_qp_{n}` cell on this row is nulled defensively** and the symbol logs
  a `logger.warning`. Not expected in practice (assets_overview should
  always supply an upcoming reportedDate).
- Same defensive rule for the annual axis (`am_anchor`).

### Quarterly column shape (asymmetric by m)

- `m = 0`: only `days_to_fiscalDateEnding_qm0`,
  `days_to_reportedDate_qm0`, and `reportTime_qm0` exist. The next
  report has not been filed yet at `d`, so no data columns are
  defined for m=0.
- `m = 1..16`: every base field has a column.
- `n = -8..4` for `_qp_{n}` estimate columns.

### Annual column shape (asymmetric by m)

- `m = 0`: only `days_to_fiscalDateEnding_am0` and
  `days_to_reportedDate_am0` exist. **No `reportTime_am0`** —
  annual EARNINGS provides no `reportTime`.
- `m = 1..4`: every base field has a column.
- `n = -2..1` for `_ap_{n}` estimate columns.
- Annual schema also lacks `estimatedEPS`, `surprise`, and
  `surprisePercentage` (those are quarterly-only).

### +/- 10 day fiscalDateEnding match window

- For `_qm{m>=1}` and the annual analogue, each statement source
  (`income_statement_q`, `balance_sheet_q`, `cash_flow_q`, `earnings_q`)
  is searched independently for a row whose `fiscalDateEnding` is within
  +/- 10 days of the m-anchor's `fiscalDateEnding`.
- **Mismatches > 10 days are logged as
  `financials_fiscalDateEnding_offcycle` and the affected source's
  fields are nulled for that m.** Other sources at the same m can still
  populate.
- Same rule for `_qp_{n}` / `_ap_{n}` against `earnings_estimates`,
  logged as `financials_estimate_offcycle`.

### Annual entries dropped if no quarterly match

An annual `fiscalDateEnding` with **no quarterly row within +/- 10 days
is dropped** from the annual axis and logged as
`financials_annual_no_quarterly_match`. (Annual EARNINGS has no
`reportedDate` / `reportTime` of its own, so it can't anchor without a
matching quarterly row.)

### Annual estimate extension (the divide-by-4 trick)

Some fiscal-quarter ends coincide with fiscal-year ends and are absent
from the quarterly estimates file but present in the annual estimates
file. Before the `_qp_{n}` lookup, the builder synthesises a quarterly
row for every annual estimate whose `fiscalDateEnding` is **not** already
present in the quarterly file (within +/- 10 days):

- **Copied verbatim:** `eps_estimate_analyst_count`,
  `revenue_estimate_analyst_count`, every
  `eps_estimate_revision_*_trailing_*_days`.
- **Divided by 4:** every other numeric field
  (`eps_estimate_average*`, `eps_estimate_high`, `eps_estimate_low`,
  `revenue_estimate_average`, `revenue_estimate_high`,
  `revenue_estimate_low`).

The synthesised rows extend the quarterly estimates frame and feed the
`_qp_{n}` lookup as well as the future-extension positions on the
m-axis.

### Anchor-only signed-day-offset columns

`days_to_fiscalDateEnding_qm{m}` is `(d - fiscalDateEnding at m).days`:
positive for past quarters (typical), slightly negative for m=0 or null.
Per-`_qp_{n}` / per-`_ap_{n}` fiscal-date offsets were intentionally not
kept: they are redundant with `days_to_fiscalDateEnding_qm{m}` /
`_am{m}`.

### qm0 / am0 PIT gate

The `_qm0` / `_am0` anchor cells describe the **next upcoming filing**
relative to `d`. They are PIT-gated against
`daily/<d'>/earnings_calendar.parquet` (largest `d' <= d`) so the
eventually-filed `reportedDate` does not leak into rows where the
announcement had not yet been published; when no calendar snapshot is
available at or before `d`, a 14-day pre-report window approximates
the typical advance-notice. See
[SPEC.md](SPEC.md) -> "qm0 / am0 PIT gating (anti-leak)" for the
exact rules.

### reportedDate consistency check (full no-op trigger)

If at any `d` the `(fiscalDateEnding, reportedDate)` pair observed for a
given fiscal end differs from a pair observed at an earlier `d` (i.e.
the provider rewrote a `reportedDate` retroactively, which should not
happen in practice), **both `financials_quarterly.parquet` and
`financials_annually.parquet` are saved as empty schema-only frames** for
that symbol and a single `financials_reportedDate_mismatch` row is
logged. `fiscalDateEnding` differences across observations do **not**
trigger the no-op (they log `financials_fiscalDateEnding_offcycle`
instead).

## 11. Macro / economic revisions: NOT PIT-correct

`price_daily` deduplication keeps the **most recent observed value**
on collisions: when the same `Date` is seen on multiple ingestion
days with different values, the latest one wins. For data sources
that **revise published values after the fact**, this means the
saved frame carries the latest revision, not the value as first
published at `d`.

This bites hardest on the `economic` asset type: BLS, BEA, and the
Fed revise CPI, GDP, unemployment, non-farm payrolls, and similar
series weeks or months after first release. Each revision is picked
up on the next ingestion and silently overwrites the prior value in
`price_daily`. Monthly `commodities` series (COPPER, WHEAT, CORN,
ALL_COMMODITIES, ...) follow the same dynamic. Daily-cadence prices
(forex, indices, cryptocurrencies, daily commodities like WTI /
BRENT) are not typically revised retroactively by the source, so
the issue is moot there.

**Consequence:** `price_daily` is **not provider-PIT** for
source-revised series. A row at `Date = d` reflects the latest
revision known at transformation time, not what was actually
published at `d`. Backtests that need true PIT macro data cannot
recover it from this frame.

This contrasts with `financials_*` (section 10), where the daily era
*is* per-row PIT-resolved. No equivalent per-row PIT resolution is
performed on `price_daily`.

## 12. etf_profile sparsity

- The historical file contributes a single row dated to the historical
  run's data-complete date; daily files contribute one row per daily
  folder they appear in.
- The frame is **sparse in time** — consumers must treat absent dates
  as "no profile snapshot taken that day", not "the profile changed".

## 13. Coverage matrix (what's present per asset type)

| Asset type | Frames built per symbol |
|---|---|
| stocks | shareprice_daily, shareprice_intraday, insider_df, sentiment_df, financials_quarterly, financials_annually |
| etfs | shareprice_daily, shareprice_intraday, etf_profile |
| forex / indices / cryptocurrencies / commodities / economic | price_daily |

Frames not present for an asset type are simply absent from the
per-symbol folder (not empty placeholders, except for the financials
no-op cases described above and the `--skip-financials` CLI flag).

## 14. transformation_report.parquet

- Per-symbol issue log at the destination root.
- **Overwritten on each full run** — reflects the current transformation,
  not a cumulative history.
- Schema: `symbol`, `asset_type`, `frame`, `issue_type`, `count`,
  `relative`, `detail`, `timestamp`. See `ISSUE_TYPES` in `_common.py`
  for the enumerated `issue_type` values.
- Useful for feature generation as a per-symbol quality signal -- symbols
  with high intraday null ratios, repeated `over_1pct` discrepancies, or
  any `financials_reportedDate_mismatch` may warrant exclusion from
  certain models.

## 15. Things feature generation MUST NOT assume

- **No pre-computed `AdjClose` / `AdjVolume` anywhere.** OHLCV is raw;
  build adjusted series from `shareprice_daily.AdjFactor` (see
  section 6) or skip the adjustment.
- **No `AdjFactor` on `shareprice_intraday`.** Join from
  `shareprice_daily` on calendar date if needed.
- **No imputation of intraday gaps or null fields.** Do it yourself.
- **No per-trading-date snapshot of insider activity.** It's a
  transaction list; project forward without leaking.
- **No retroactive amendment tracking in `insider_df` or
  `sentiment_df`.** Most-recent-source-wins; offline raw replay if you
  need amendment history.
- **No completeness in the most recent ~hour-plus of `sentiment_df`.** The
  AV news feed backfills late; the trailing edge is systematically sparse
  until a later run's 7-day overlap fills it. See section 9.
- **No guarantee that `etf_profile` has a row on every trading date.**
  It's sparse.
- **No `Volume` for forex / indices / commodities.** Column is null by
  construction. Don't filter on it.
- **No data columns at `m=0` in `financials_quarterly`** beyond
  `days_to_fiscalDateEnding_qm0` and `reportTime_qm0`. The next report
  hasn't been filed at `d` yet.
- **No `reportTime_am0`** on the annual axis at all.
- **No commodities `unit` column.** Dropped at transformation; if needed,
  it belongs in `metadata.json` via a future scalar field.

## 16. Things feature generation CAN rely on

- Schema-exact frames: dtypes and column names match
  `AssetData_specifications.md` byte-for-byte, or the load fails.
- `shareprice_daily` has **no nulls** in any Float32 column.
  `AdjFactor` in particular is always populated; on no-event rows it
  is exactly `1.0`, and the first row is `1.0` by convention.
- Every `shareprice_intraday` row's calendar date appears in
  `shareprice_daily.Date` for the same symbol.
- Every symbol in any catalog appears exactly once in
  `assets_overview.parquet`.
- `report_table` orders by `reportedDate` (PIT-correct), not by
  `fiscalDateEnding`, so the m-axis is "what was filed when" — late
  filers and out-of-order fiscal ends are correct, not bugs.
- The d-PIT financials snapshot resolution does not leak restated values
  backward.
- Per-symbol folder is all-or-nothing — if `metadata.json` exists, every
  implemented frame for that asset type is present (possibly empty for
  `financials_*` under the no-op cases).

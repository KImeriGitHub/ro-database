# Data Transformation

Transforms the raw parquet files written by `historical_data_setup/` and
`daily_data_service/` into the canonical `AssetData` dataclasses defined in
[AssetData.py](AssetData.py), one instance per symbol, persisted to disk via
[AssetDataService.py](AssetDataService.py)'s `save_to`.

The output of this step is the dataset that feature engineering and strategy
research consume. Raw parquet files under `historical/` and `daily/` are never
read again past this step.

## What this module does

1. Builds `assets_overview.parquet` at the destination root - a single index
   table (`symbol`, `assetType`, `about`, `reportedDate`, `timeOfTheDay`,
   `sector`) covering every symbol across every asset type. Every later
   step iterates this table.
2. For each `(asset_type, symbol)` it builds an instance of the matching
   `AssetData` subclass and writes it to `<dest>/<asset_type>/data_<symbol>/`
   (one folder per symbol, exactly the layout produced by `save_to`). The
   `data_` prefix mirrors the per-symbol filename prefixes used in
   `historical/` and `daily/`: it prevents Windows reserved names (`CON`,
   `PRN`, `AUX`, `NUL`, `COM0-9`, `LPT0-9`) from colliding with real
   tickers like `PRN` or `CON` when the dest tree is materialised on
   Windows. The symbol component itself is routed through
   `historical_data_setup._common.fs_symbol`, so a slash-class ticker
   like `BC/PB` becomes `data_BC%2FPB/` (one directory) rather than
   splitting into `data_BC/PB/`. See
   [`historical_data_setup/README.md`](../historical_data_setup/README.md#filesystem-safe-symbol-encoding-fs_symbol)
   for the encoding rules.

The schemas of the per-frame parquet files (`shareprice_daily.parquet`,
`shareprice_intraday.parquet`, `price_daily.parquet`, `etf_profile.parquet`,
...) are the ones registered in `AssetDataService.SCHEMAS` and documented in
[AssetData_specifications.md](AssetData_specifications.md).

## Output layout

```
<dest>/
├── assets_overview.parquet
├── transformation_report.parquet      # per-symbol issue log, see "Logging" below
├── stocks/
│   └── data_<SYMBOL>/                 # data_ prefix for Windows-reserved-name safety; <SYMBOL> is fs_symbol-encoded
│       ├── metadata.json              # {ticker, about, sector, _asset_type}
│       ├── shareprice_daily.parquet
│       ├── shareprice_intraday.parquet
│       ├── insider_df.parquet
│       ├── sentiment_df.parquet
│       ├── financials_quarterly.parquet
│       └── financials_annually.parquet
├── etfs/
│   └── data_<SYMBOL>/
│       ├── metadata.json
│       ├── shareprice_daily.parquet
│       ├── shareprice_intraday.parquet
│       └── etf_profile.parquet
├── forex/
│   └── data_<SYMBOL>/
│       ├── metadata.json
│       └── price_daily.parquet
├── indices/data_<SYMBOL>/...
├── cryptocurrencies/data_<SYMBOL>/...
├── commodities/data_<SYMBOL>/...
└── economic/data_<SYMBOL>/...
```

The per-symbol folder format (metadata.json + one parquet per frame) is
defined in `AssetDataMixin.save_to` and round-trippable via
`AssetDataMixin.load_from`.

## assets_overview.parquet

Schema:

| Column        | Type  | Source |
|---------------|-------|--------|
| symbol        | Utf8  | `catalog/{asset_type}.parquet:symbol` |
| assetType     | Utf8  | one of stocks, etfs, forex, indices, cryptocurrencies, commodities, economic |
| about         | Utf8  | `catalog/{asset_type}.parquet:name` (empty string if absent) |
| reportedDate  | Date  | `earnings_calendar.parquet:reportedDate` (next upcoming row per symbol; null if absent). Resolved from the newest `daily/<YYYY-MM-DD>/earnings_calendar.parquet`, falling back to `historical/earnings_calendar.parquet`. |
| timeOfTheDay  | Utf8  | `earnings_calendar.parquet:timeOfTheDay` (empty string if absent). Same resolver as `reportedDate`. |
| sector        | Utf8  | `catalog/stocks.parquet:sector` verbatim (empty string for non-stocks). The catalog is the canonicalization point: downstream code (e.g. `sector_to_index` for `StockData.sector`) maps these strings against `CANONICAL_SECTORS` in AssetData.py and falls through to `Other` on unknown values, so any non-canonical strings stored here silently degrade to `Other` later. |

Every symbol from every asset catalog appears exactly once. `about`,
`timeOfTheDay`, and `sector` use `""` for missing values; `reportedDate` uses
`null` (Date columns can't hold an empty string).

## Per-frame transformation rules

Source files live under:

- `historical/<asset_type>/<endpoint>/{prefix}{symbol}.parquet`
- `daily/YYYY-MM-DD/<asset_type>/<endpoint>/{prefix}{symbol}.parquet`

with `{prefix}` per `historical_data_setup._common.ASSET_TYPE_FILE_PREFIX`.
For each symbol every available file (one historical + one per daily folder)
is read and concatenated, then transformed into the target frame.

### Source enumeration & memory discipline

For every `(asset_type, endpoint)` we scan the source tree **once** at the
start of the asset-type pass and build a `dict[str, list[Path]]` mapping
`symbol -> [historical_path, daily_path_1, daily_path_2, ...]` (sorted by
folder date, historical first). This avoids `os.listdir` on `daily/` for
each of the ~24k symbols. For stocks/etfs all relevant indexes
(prices_daily, prices, plus per asset type etf_profile / insider /
sentiment / the 10 fundamentals indexes) are built up front and kept for
the duration of the combined per-symbol pipeline (Phases 3-6c) on that
asset type, then dropped before the next asset type runs. They are never
shared across asset types.

The transformation is streamed symbol by symbol:

1. Look up the symbol's source paths in the prebuilt dict.
2. Read, concat, dedup, transform, cast to `SCHEMAS[name]`.
3. Build the `AssetData` instance, `save_to(<dest>/<asset_type>/data_<SYMBOL>/)`.
4. Drop every intermediate (source frames, instance) before the next
   iteration. No accumulation of per-symbol state across the loop.

`AdjFactor` is computed inline on `shareprice_daily` from each row's
`Close`, the prior `Close`, `SplitCoefficient`, and `DividendAmount`,
and is materialised as a column on the saved frame; no separate
factor frame is held across phases. Phase 4 (`shareprice_intraday`)
no longer needs it - the intraday frame keeps raw OHLCV and
downstream consumers join on calendar date if they want adjusted
returns.

### Source overlap and dedup (non-optional)

Every daily-interval endpoint in the daily ingest (`prices`,
`prices_daily`, `forex`, `cryptocurrencies`, `indices`, the daily group
of `commodities`, the daily indicators of `economic`) intentionally
captures a trailing 7-day window per run
(`(min(previous_date, folder_date - 7d), folder_date]`), so consecutive
`daily/<date>/.../<symbol>.parquet` files for the same symbol overlap by
up to 6 days. This is by design: it lets a single successful daily run
recover the last few days of bars even when intermediate runs failed for
that symbol. See
[daily_data_service/README.md](../daily_data_service/README.md#truncation-rules).

**Every frame builder that concats across daily folders MUST dedup**, or
the output frames will contain duplicate `(Date)` / `(Datetime)` rows.
The canonical place is step 3 of each per-frame section below
("Deduplicate on `Date`/`Datetime`"), which delegates to
[`frames/_dedup.py:dedup_with_discrepancy_log`](frames/_dedup.py) and
logs `dedup_value_discrepancy_under_1pct` / `over_1pct` rows to
`transformation_report.parquet` whenever overlapping rows disagree on a
Float32 field. Skipping or replacing this step with a naive concat is a
correctness bug; cross-folder overlap will reach downstream consumers.

The dedup helper takes a `keep` argument that selects the winner on
collisions:

* **`keep="last"`** (price frames -- `price_daily`, `shareprice_daily`,
  `shareprice_intraday`): the most recent daily snapshot wins.
  Restatements of OHLCV (split / dividend backfills, exchange replays)
  overwrite the older value. This trades PIT integrity for data quality
  on the assumption that recent OHLCV revisions are corrections.
* **`keep="first"`** (everything else -- `insider_df`, `sentiment_df`,
  `etf_profile`): the earliest snapshot to capture the row wins. PIT-
  correct: the value first observed at time `T` is what a strategy would
  have seen at `T`; later restatements are dropped (still logged).

`insider_df` and `sentiment_df` additionally pass `flag_under_1pct=False`
to the helper. Their Float32 fields drift between snapshots by sub-1%
as a normal matter of operation (rewritten share counts, AV's
sentiment model rescoring already-published articles by tiny
amounts); the under-1pct classification on these frames is noise, so
it is never recorded. Over-1pct entries still fire and represent real
restatements worth reviewing.

#### Historic-snapshot boundary suppression

`price_daily`, `shareprice_daily`, and `shareprice_intraday` pass
`suppress_historic_boundary=True` to the dedup helper. The historic
snapshot's last bar is routinely a *partial bar*: 24/7 markets
(cryptocurrencies) never close, and any historic pull captured mid-
session (forex on a weekend, equities during regular trading hours,
intraday at any moment except the session close) carries OHLCV that
reflects only the part of the day already elapsed at download time.
When a later daily folder re-pulls the same date it gets the complete
bar, and the resulting discrepancy is structural rather than a real
restatement.

The suppression rule:

- Identify the *boundary key* = ``max(key)`` across rows from the
  historic source (``_source_order == 0``).
- For duplicate keys at the boundary, drop source-0 rows from the
  discrepancy classification (the dedup itself is unaffected -- with
  `keep="last"` the partial historic value is already discarded in
  favor of the daily value).
- All other duplicate keys are classified as before. In particular,
  a daily-vs-daily disagreement on the boundary date (two later
  snapshots disagreeing among themselves) still surfaces, because
  the suppression only filters source-0 rows.

A single boundary key is suppressed per symbol, so the rule has
narrow blast radius: a `cryptocurrencies/BTC/price_daily` run that
captures one boundary partial bar (e.g. the historic snapshot's
final Sunday) silently keeps the daily-source value for that date
without emitting a `dedup_value_discrepancy_*` row, while interior
overlaps (where historic and daily disagree on a date that is not
the boundary) continue to log. Composite-key frames
(`insider_df`, `sentiment_df`) do not use the rule; the helper
ignores `suppress_historic_boundary` when called with a multi-
column key.

The remaining endpoints (fundamentals, `insider`, `sentiment`,
`etf_profile`, monthly `commodities`, non-daily `economic`) do not use
the 7-day floor, but their builders still dedup -- historical and daily
snapshots can independently disagree on a value, and any append-only
restatement detection produces overlapping rows the same way.

### Dtype discipline

Every per-frame builder ends with an explicit cast/select against
`SCHEMAS[name]` (see `AssetDataService.SCHEMAS`). Any column whose dtype
or name does not match the registered schema is a fatal error: the cast
raises and the symbol's transformation aborts (with a row in
`transformation_report.parquet`). We do not silently coerce, drop, or
re-order. Drift in the upstream parquet schemas should surface
immediately, not be papered over here.

### shareprice_daily (stocks, etfs)

Source: `historical/<a>/prices_daily/` + `daily/*/<a>/prices_daily/` for
`a in {stocks, etfs}`.

1. Concat all source frames.
2. Sort by `Date` ascending.
3. Deduplicate on `Date`. Discrepancy logging: when two rows share a `Date`,
   compare every Float32 column. If any pair differs, record one row in
   `transformation_report.parquet`:
   - `issue_type = "dedup_value_discrepancy_under_1pct"` if every difference
     is `<1%` of the larger value (i.e. acceptable noise; the daily snapshot
     wins).
   - `issue_type = "dedup_value_discrepancy_over_1pct"` if any field differs
     by `>=1%` (the row is still kept, daily snapshot wins, but the symbol
     is flagged for review).
4. Compute the **single-day** `AdjFactor` column. OHLCV stays raw -
   no `AdjClose` / `AdjVolume` is written. The formula (CRSP-style,
   anchored on the prior close):
   - `AdjFactor[i] = SplitCoefficient[i] * Close[i-1] / (Close[i-1] - DividendAmount[i])`
     for `i >= 1`.
   - `AdjFactor[0] = 1.0` (no prior close to anchor on).
   - On rows with no split and no dividend (`SplitCoefficient = 1`,
     `DividendAmount = 0`), the formula reduces to `AdjFactor = 1.0`.
   - The factor is per-day - **no cumulative product** is taken here.
     Reconstructing `AdjClose` (or any other cumulative adjustment) is
     deliberately left to downstream consumers, who can choose their
     own as-of date and so control the lookahead trade-off. See
     [AssetData_design_choices.md](AssetData_design_choices.md)
     section 6 and "Lookahead bias" below.
5. Drop rows where any Float32 column is null after the above. Record the
   per-symbol drop count in `transformation_report` (`issue_type =
   "dedup_dropped_null_row"`).
6. Cast to `SCHEMAS["shareprice_daily"]` and write.

`Volume` may be null in the source (e.g. some FRD-derived rows). A null
Volume that survives concat will trigger a row drop in step 5, which is
intentional - downstream code can rely on `shareprice_daily` having no
nulls.

### shareprice_intraday (stocks, etfs)

Source: `historical/<a>/prices/` + `daily/*/<a>/prices/` for `a in
{stocks, etfs}`.

1. Concat all source frames; rename source `Date` (`pl.Datetime`) to
   `Datetime` to match the schema.
2. Sort by `Datetime` ascending.
3. Deduplicate on `Datetime`. Discrepancy logging same as
   `shareprice_daily` (under_1pct vs over_1pct).
4. Drop rows whose calendar date is not present in `shareprice_daily.Date`
   for the same symbol. Record:
   - `issue_type = "intraday_orphan_date_dropped"`, `count = dropped rows`,
     `relative = dropped / total`.
   The opposite case (a `Date` in shareprice_daily with no matching
   intraday) is normal and not logged.
5. **Do not drop null Float32 rows.** Record per-symbol the count and
   ratio of null fields (`issue_type = "intraday_null_field"`).
6. Cast to `SCHEMAS["shareprice_intraday"]` (raw `Datetime`,
   `Open/High/Low/Close/Volume`) and write. **No adjustment is
   applied.** Consumers that need adjusted intraday returns join
   `shareprice_daily.AdjFactor` on the calendar date of `Datetime`
   themselves.

> **Note for downstream consumers.** The intraday frame may contain rows
 with null OHLCV values, gaps in time coverage, and uneven bar density.
 Imputing missing values, removing rows around big gaps, and otherwise
 cleaning the time grid is the responsibility of the feature-engineering
 step that consumes `AssetData`, not this transformation. We surface the
 nulls (via `transformation_report.parquet`) but do not mutate them.

### price_daily (forex, indices, cryptocurrencies, commodities, economic)

Source: `historical/<a>/` + `daily/*/<a>/` (no nested endpoint folder for
these asset types).

1. Concat all source frames.
2. Sort by `Date` ascending.
3. Deduplicate on `Date`, with discrepancy logging identical to
   `shareprice_daily` step 3.
4. Drop rows where any OHLC field (`Open`, `High`, `Low`, `Close`) is
   null. **Volume nulls do not trigger a drop** - forex / indices /
   commodities carry no Volume in the source at all (the column is
   synthesised as null before the cast), and dropping on a Volume null
   would wipe every row. Cryptocurrencies are the only flat asset type
   that carry a real Volume, so for them the null-Volume rows that do
   slip through stay in the frame rather than being dropped. Log the
   OHLC-driven drop count via `dedup_dropped_null_row`.
5. Cast to `SCHEMAS["price_daily"]` and write.

Commodities and economic indicators that arrive as `(Date, value, unit)`
or `(Date, value)` are mapped: `value -> Close`; `Open/High/Low <- Close`;
`Volume = null`. The `unit` column from commodities is dropped (the
`AssetData.price_daily` schema has no place for it; if needed later it
belongs in `metadata.json` via a new scalar field).

### etf_profile (etfs)

Source: `historical/etfs/etf_profile/` + `daily/*/etfs/etf_profile/`.

1. Concat all source frames; rename `date` -> `Date`.
2. Sort by `Date` ascending; deduplicate on `Date` (discrepancy logging same
   as above).
3. Drop the `inception_date` column (present in source, absent from
   target schema).
4. Cast `leveraged` (`pl.String` "YES"/"NO") to `pl.Categorical`.
5. Cast to `SCHEMAS["etf_profile"]` and write.

The historical file contributes a single row dated to the historical run's
data-complete date; the daily files contribute one row per daily folder
they appear in. The resulting `etf_profile` is sparse in time
- consumers must treat absent dates as "no profile snapshot taken that day".

### insider_df (stocks)

Source: `historical/stocks/insider/` + `daily/*/stocks/insider/`.

Per source row, the raw schema carries
`(transactionDate, executive, executive_title, security_type,
acquisition_or_disposal, shares, share_price)`. The transformed schema
keeps only the modelling-relevant columns (see
[AssetData_specifications.md](AssetData_specifications.md) section
`### insider_df`): `Date`, `Executive_role`, `AcqDis`, `Shares`.

1. Read every source file. Concat with `attach_source_order`.
2. Deduplicate on the composite key
   `(transactionDate, executive, security_type)` with
   `keep="first"` (earliest source wins -- the snapshot that first
   captured the row keeps its values; later restatements are
   dropped). Discrepancy logging on the Float32 fields
   (`shares`, `share_price`) emits only
   `dedup_value_discrepancy_over_1pct` -- under-1pct is suppressed
   because tiny drifts between snapshots are normal noise on this
   frame; only large restatements are worth surfacing. Dedup happens
   **before** the role mapping so two source titles that collapse to
   the same role for the same executive are still surfaced as a
   discrepancy if the underlying values differ.
3. Map `executive_title` -> `Executive_role` via the ordered
   case-insensitive substring rule list. The first matching rule
   wins. Order is most-specific first (so e.g. "Chief Accounting
   Officer" hits CAO before the generic "chief " rule, "President &
   CFO" hits CFO before CEO). Empty / null / unmatched titles fall
   through to `"Other"`. The rule order **is** the spec - see
   [AssetData_specifications.md](AssetData_specifications.md) and
   `_INSIDER_ROLE_RULES` in `frames/insider.py`.
4. Map `acquisition_or_disposal` -> `AcqDis` verbatim
   (`"A"` / `"D"`). Rows with any other value (or null) are dropped.
5. Drop rows where `Shares` is null (or `AcqDis` is invalid). Log via
   `dedup_dropped_null_row`.
6. Set `Date := transactionDate` and sort by `Date` ascending.
7. Cast to `SCHEMAS["insider_df"]`.

`Shares` is a raw count, not a USD amount; `share_price` is dropped
from the schema. The output is a chronological list of transactions,
**not** a per-trading-date snapshot. Avoiding lookahead leakage is the
responsibility of the feature-generation step that consumes
`StockData`.

> **Note on retroactive amendments.** Late retroactive changes to
> already-observed insider rows are dropped: the dedup helper uses
> `keep="first"` per `(transactionDate, executive, security_type)`,
> so an amendment that changes only `shares` or `share_price`
> retains the earliest captured value. Amendments that move a value
> by >=1% surface as `dedup_value_discrepancy_over_1pct` in the
> transformation report; sub-1% drift is suppressed as noise. If you
> need to inspect amendments, raw `daily/*/stocks/insider/` retains
> every snapshot and PIT replay is possible offline.

### sentiment_df (stocks)

Source: `historical/stocks/sentiment/` + `daily/*/stocks/sentiment/`.

Per source row, the raw schema is the full NEWS_SENTIMENT response
(time_published, ticker, scores, title, url, authors, summary,
banner_image, source labels, plus 15 topic relevance columns). The
transformed schema keeps only the numeric scores plus `Datetime` (see
[AssetData_specifications.md](AssetData_specifications.md) section
`### sentiment_df`); titles, urls, authors, and full text are
dropped at the cast step.

1. Read every source file. If a `ticker` column is present, filter
   defensively to `ticker == symbol` (the per-symbol files already
   filter upstream, but the column is not guaranteed to be present
   on every file).
2. Rename `time_published` -> `Datetime`.
3. Concat via `attach_source_order` and deduplicate on
   `(Datetime, url)` with `keep="first"` (earliest source wins -- the
   sentiment scores first published for an article persist; later
   model rescores are dropped). Discrepancy logging covers every
   Float32 column (`ticker_relevance_score`,
   `ticker_sentiment_score`, `overall_sentiment_score`, plus the 15
   topic relevance columns), but only the over-1pct bucket is
   recorded -- AV's sentiment model regularly nudges scores by sub-1pct
   between snapshots and that drift is suppressed as noise. Two
   articles published in the same minute with different urls survive
   as distinct rows.
4. Sort by `Datetime` ascending.
5. Cast to `SCHEMAS["sentiment_df"]`. The cast drops the source
   columns absent from the target schema (`url`, `title`, `summary`,
   `authors`, `banner_image`, `source`, `category_within_source`,
   `source_domain`, `ticker_sentiment_label`,
   `overall_sentiment_label`, plus `ticker` if present).

The historical sentiment endpoint also writes
`ALL_MESSAGES.parquet` next to the per-symbol files; the source
enumeration filename match (`{prefix}{symbol}.parquet`) skips it
naturally.

### financials_quarterly, financials_annually (stocks)

Per-symbol fundamentals come from five endpoints, each as `_annual` /
`_quarterly` parquet pairs under `historical/stocks/<endpoint>/` and
`daily/<YYYY-MM-DD>/stocks/<endpoint>/`: `income_statement`,
`balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`.

Unlike `shareprice_daily`, `shareprice_intraday`, `insider_df`, and
`sentiment_df` (which concatenate every available snapshot),
fundamentals are looked up **per row date `d`** as a point-in-time
snapshot. The row axis is `shareprice_daily.Date` (so a stock with
no prices produces empty financials frames).

#### Per-row PIT snapshot resolution

For each row date `d`:

- if `daily/<d>/stocks/<endpoint>/<prefix><sym>{_annual,_quarterly}.parquet`
  exists, use those files for the `d` row only;
- else if `daily/<d'>/...` exists for the largest `d' < d`, use that
  day's files and log `financials_snapshot_fallback`;
- else use the historical files.

This captures retroactive amendments without leaking restated values
into pre-restatement rows.

#### Per-symbol report_table

Before populating any cell, the builder constructs a per-symbol
`report_table` of `(reportedDate, fiscalDateEnding, reportTime)`.
`reportedDate` is the canonical chronological axis (PIT-correct:
m-position represents "what had been filed when"); rows with a
known `reportedDate` are sorted ascending by it, and
future-extension rows with null `reportedDate` are appended at the
tail in `fiscalDateEnding` ascending order.

Sources:

1. **Past entries.** Every row in the in all snapshots of
   `earnings/stocks_SYMBOL_quarterly.parquet` contributes
   `(reportedDate, fiscalDateEnding, reportTime)`. Consider 
   here the 'reportedDate consistency check' below.
2. **Next-upcoming entry.** From `assets_overview.parquet`'s
   `(reportedDate, timeOfTheDay)` for the symbol, paired with the
   smallest `fiscalDateEnding` in the most recent extended
   `earnings_estimates/stocks_SYMBOL_quarterly.parquet` (see below) that is strictly
   greater than the largest past `fiscalDateEnding`. If no such
   estimate exists, the next-upcoming entry is omitted.
3. **Further-future entries.** From the most recent extended
   `earnings_estimates/stocks_SYMBOL_quarterly.parquet` (see below) for any `fiscalDateEnding`
   strictly later than the next-upcoming entry. `reportedDate` and
   `reportTime` are null for these rows. Further-future entries only serve as
   anchors for the `_qp_{n}` fields.

`reportTime` is normalised to the categorical labels
{`pre-market`, `post-market`, `other`}. Empty strings, nulls, and
unknown labels map to `other`. Late filers (a quarter whose
`reportedDate` post-dates a later quarter's) are fine; 
their `fiscalDateEnding` will appear out-of-sequence relative 
to neighbours, which is the PIT-correct ordering.

#### Quarterly cell mapping

The `_qm{m}` columns in `SCHEMAS["financials_quarterly"]` are
asymmetric by m:

- `m = 0`: only `days_to_fiscalDateEnding_qm0`,
  `days_to_reportedDate_qm0`, and `reportTime_qm0` exist. The next
  report has not been filed yet at `d`, so no data columns are
  defined for m=0.
- `m = 1..16`: every base field
  (`days_to_fiscalDateEnding`, `days_to_reportedDate`, `reportTime`,
  plus all statement / earnings fields) has a column.

For each row date `d` in `shareprice_daily.Date`:

- `m_anchor = smallest position i in report_table with
  reportedDate[i] > d`. If no such position exists, fall back to
  one of two cases keyed on `(d - max(reportedDate)).days` against
  `NO_ANCHOR_TOLERANCE_DAYS` (60):
  - **Soft no-anchor** (`d - max(reportedDate) < 60 days`):
    `m_anchor = n_known_q` (the count of known reportedDate
    entries; past-the-end of the known prefix). Position `pos0`
    for `qm0` is out of range so the qm0 anchor cells null, but
    `qm{m>=1}` walks back through past quarters from the latest
    known reportedDate and `qp_{n}` can use future-extension rows.
    This is the common case when `assets_overview` does not
    supply an upcoming reportedDate (which the integration tests
    show happens in practice). Logged at `logger.info`.
  - **Hard no-anchor** (`d - max(reportedDate) >= 60 days`, or no
    known reportedDate exists at all): the entire row's financials
    columns (every `_qm{m>=0}` and every `_qp_{n}` cell) are nulled
    defensively. Logged at `logger.info` (common for delisted /
    dormant symbols where `d` runs past the last known filing; not
    actionable on its own).
- For each `m in 1..16`, position `i = m_anchor - m`. Out-of-range
  positions null all `_qm{m}` columns for that m.
- `days_to_fiscalDateEnding_qm{m} = (d - report_table.fiscalDateEnding[i]).days`
  cast to Float32. Positive when the fiscal quarter has already
  ended (typical past quarters); slightly negative for m=0 on
  rows where the upcoming quarter has not yet ended.
- `days_to_reportedDate_qm{m} = (d - report_table.reportedDate[i]).days`
  cast to Float32. Positive when the report has already been
  filed (m>=1, typical); negative for m=0 (the next upcoming
  report by definition has `reportedDate > d`).
- `reportTime_qm{m}` is `report_table.reportTime[i]`.
- For `m >= 1`, the remaining `_qm{m}` columns are pulled from the
  d-PIT snapshot: each of `income_statement_q`, `balance_sheet_q`,
  `cash_flow_q`, and `earnings_q` is searched independently for
  the row whose `fiscalDateEnding` is within +/- 10 days of
  `report_table.fiscalDateEnding[i]`. Mismatches >10 days are
  logged as `financials_fiscalDateEnding_offcycle` and the
  affected source's fields are null for that m.
- For each `n in -8..4`, position `i = m_anchor + n`. Out-of-range
  positions null all `_qp_{n}` columns. The `_qp_{n}` columns are
  pulled from the extended `earnings_estimates/stocks_SYMBOL_quarterly.parquet` (see
  below) by matching `fiscalDateEnding` within +/- 10 days of
  `report_table.fiscalDateEnding[i]`; mismatches >10 days are
  logged as `financials_estimate_offcycle`.

#### qm0 / am0 PIT gating (anti-leak)

The `_qm0` / `_am0` anchor cells describe the **next upcoming report**
relative to `d`. The eventually-filed `reportedDate` for that report
was usually announced a few weeks before the filing, not years in
advance -- so using `report_table` directly would leak announcement
knowledge into rows where it had not yet been published. Resolution
proceeds in three branches:

1. **A `daily/<d'>/earnings_calendar.parquet` exists at or before `d`**
   (largest `d' <= d`, same fallback convention as the statement
   files). The snapshot is authoritative:
   - **qm0**: locate the row in the calendar for this symbol with the
     **largest `reportedDate`**. If it is strictly greater than `d`,
     populate `days_to_fiscalDateEnding_qm0`, `days_to_reportedDate_qm0`,
     and `reportTime_qm0` from that row (`fiscalDateEnding`,
     `reportedDate`, `timeOfTheDay` normalised). Otherwise (`reportedDate
     <= d`, or no row exists for the symbol) null all three.
   - **am0**: locate the row in the calendar for this symbol whose
     `fiscalDateEnding` matches the anchor's `fiscalDateEnding`
     (`rt_a_rows[pos0]`) within +/- 10 days. If found and its
     `reportedDate > d`, populate `days_to_fiscalDateEnding_am0` and
     `days_to_reportedDate_am0` from that row. Otherwise null both.
2. **No `earnings_calendar.parquet` exists for any `d' <= d`**
   (the historical regime, before the daily pipeline produced any
   calendar files). Apply the **14-day pre-report rule** using the
   `report_table` anchor at `pos0`: if `1 <= (reportedDate[pos0] - d).days
   <= 14`, populate the qm0 / am0 cells from `report_table` as before.
   Outside the 14-day window null the cells. This approximates the
   typical advance-notice window that companies publish their earnings
   date, without conceding a PIT-honest gate.
3. **`pos0` is out of range** (soft no-anchor: `report_table` has no
   row with `reportedDate > d`). qm0 may still be populated when branch
   1 applies (earnings_calendar provides the gate independently); am0
   relies on `rt_a_rows[pos0]` for the fde match, so am0 nulls in this
   case.

`earnings_calendar.parquet` is a global file (all symbols) covering a
6-month horizon -- see
[historical_data_setup/SPEC.md](../historical_data_setup/SPEC.md#earnings_calendar).
The historical `historical/earnings_calendar.parquet` is intentionally
**not** used for the gate: it carries no PIT timestamp, so applying it
to arbitrary historical row dates would re-introduce the leak.

#### Annual cell mapping

`report_table_annual` is built by walking annual `fiscalDateEnding`
values from `earnings/stocks_SYMBOL_annual.parquet`. For each annual
`fiscalDateEnding F_a`, the matching quarterly row (within +/- 10
days of `F_a`) supplies `reportedDate` and `reportTime`. Annual
entries with no quarterly match within 10 days are dropped and
logged as `financials_annual_no_quarterly_match`. Future-annual
entries from `earnings_estimates/stocks_SYMBOL_annual.parquet` extend the table the same
way as the quarterly case, with `reportedDate` and `reportTime`
left null. Sort key is `reportedDate` ascending (with future
entries at the tail by `fiscalDateEnding`), same convention as
quarterly.

The `_am{m}` columns are asymmetric by m:

- `m = 0`: only `days_to_fiscalDateEnding_am0` and
  `days_to_reportedDate_am0` exist. (Annual EARNINGS provides no
  `reportTime`, so unlike the quarterly schema there is no
  `reportTime_am0`.) `reportedDate` for the annual axis is
  inherited from the matched quarterly row at the same
  fiscalDateEnding (see "Annual cell mapping" report_table_annual
  construction).
- `m = 1..4`: every base field has a column.

For each row date `d`, the `am_anchor` and `_am{m}` / `_ap_{n}`
resolution mirrors the quarterly case (m in 0..4, n in -2..1),
pulling from the d-PIT `*_a.parquet` files. `_am0` follows the same
PIT-gating rule as `_qm0` described above (fde-matched lookup against
the annual anchor, with the 14-day fallback when no
`earnings_calendar.parquet` snapshot is available). The same two-tier
no-anchor rule applies on the annual axis: when no annual
`reportedDate > d` exists in `report_table_annual`, the soft case
(`d - max(reportedDate) < 60 days`) sets `am_anchor = n_known_a`
so `am0` nulls but `am{m>=1}` and `ap_{n}` stay populated; the
hard case nulls every `_am{m>=0}` and `_ap_{n}` cell. Annual
EARNINGS does not provide `reportTime`, `estimatedEPS`,
`surprise`, or `surprisePercentage` (those are quarterly-only and
absent from `SCHEMAS["financials_annually"]`).

#### Annual estimate extension

Some fiscal-quarter ends coincide with fiscal-year ends. In those
cases the upstream `earnings_estimates/stocks_SYMBOL_quarterly.parquet` may have
no entry at that `fiscalDateEnding` while
`earnings_estimates/stocks_SYMBOL_annual.parquet` does.

Before the `_qp_{n}` lookup, the builder synthesises a quarterly
row for every annual estimate whose `fiscalDateEnding` is **not**
already present in the quarterly file (within +/- 10 days):

- `eps_estimate_analyst_count`, `revenue_estimate_analyst_count`,
  and every `eps_estimate_revision_*_trailing_*_days` field are
  copied verbatim;
- every other numeric field (`eps_estimate_average*`,
  `eps_estimate_high`, `eps_estimate_low`,
  `revenue_estimate_average`, `revenue_estimate_high`,
  `revenue_estimate_low`) is divided by 4.

The synthesised rows are merged into the quarterly estimates frame
and thus extend it. This extension is used both by the `_qp_{n}` lookup
and by the `report_table` future-fiscalDateEnding extension.

#### reportedDate consistency check

If any d-PIT snapshot's `(fiscalDateEnding, reportedDate)` pair
differs from an earlier-seen pair for the same `fiscalDateEnding`
(i.e. the provider rewrote a `reportedDate` retroactively, which
should not happen in practice), both
`financials_quarterly.parquet` and `financials_annually.parquet`
are saved as **empty schema-only frames** for that symbol and a
single `financials_reportedDate_mismatch` row is logged.
`fiscalDateEnding` differences across snapshots are logged as
`financials_fiscalDateEnding_offcycle` and do not trigger the
no-op.

In practice this check only ever fires on the quarterly path:
Alpha Vantage's annual EARNINGS payload contains only
`fiscalDateEnding` and `reportedEPS`, so every annual row's
`reportedDate` is null and the consistency comparison is skipped
(rows with null `reportedDate` are tolerated and never trigger
the no-op). Mixed null-vs-non-null `reportedDate` values for the
same `fiscalDateEnding` across snapshots are also tolerated for
the same reason: a snapshot that simply has not yet picked up the
filing date does not look like a rewrite.

#### CLI

`--skip-financials` skips this builder for stocks; the frames are
saved as empty schema-only placeholders, like before this phase
landed. Useful for fast iteration during development.

#### Issue types

| Issue type | Trigger |
|---|---|
| `financials_reportedDate_mismatch` | retroactive change in reportedDate for an already-known fiscalDateEnding; triggers a full no-op for the symbol |
| `financials_fiscalDateEnding_offcycle` | quarterly fiscalDateEnding in IS / BS / CF / E differs from the report_table anchor by >10 days, while still inside the endpoint file's covered fde range (anchors older or newer than the file's coverage are plain absence, not off-cycle, and are suppressed) |
| `financials_snapshot_fallback` | no `daily/<d>/` for a row date d; fell back to most recent earlier daily snapshot |
| `financials_estimate_offcycle` | earnings_estimates fiscalDateEnding differs from the anchor's by >10 days, while still inside the estimates file's covered fde range |
| `financials_no_earnings_file` | no `earnings/SYMBOL_quarterly.parquet` anywhere; both financials frames empty |
| `financials_annual_no_quarterly_match` | annual fiscalDateEnding has no quarterly match within 10 days; annual entry dropped |

## Lookahead bias on the historical period

OHLCV in `shareprice_daily` and `shareprice_intraday` is **raw and
unadjusted**, and `AdjFactor` is a single-day multiplier (no
cumulative product). That keeps any future-looking adjustment out of
the stored frame: a consumer that wants a Yahoo / CRSP-style
`AdjClose` must take a cumulative product, and is therefore the one
choosing the as-of date that bounds the lookahead. See
[AssetData_design_choices.md](AssetData_design_choices.md) section 6
for the formula and a worked PIT-correct reconstruction.

Two practical consequences:

- **Adjusted intraday returns require a join.** `shareprice_intraday`
  carries no Adj* and no `AdjFactor`; consumers join
  `shareprice_daily.AdjFactor` on the calendar date of `Datetime`.
- **Fundamentals still depend on PIT-snapshot accumulation.** See
  "PIT readiness" below; this is unrelated to the price-adjustment
  story.

### PIT readiness

The transformed frames in this module fold every available `daily/<date>/`
snapshot into a single time series; nothing here knows about restatement
boundaries. As a rule of thumb, treat the transformed dataset as
**not yet productive** until at least roughly 3 months of `daily/<date>/`
folders have accumulated. Below that, the PIT layer is too sparse for the
fundamentals frames to reflect meaningful as-of-date behaviour, and most
backtests that rely on `financials_quarterly` / `financials_annually`
will silently behave as if the data had always been "latest available."
There is no programmatic gate on this; it is a calendar-time prerequisite
for users of the output.

For consumers in the next repo, the boundary between historical-baseline
financials (restatements possibly baked in) and PIT-honest financials
is approximated by `min(etf_profile.Date)` taken across the etf
catalog - see
[AssetData_design_choices.md](AssetData_design_choices.md) section 10
"Historical baseline carries restatements".

`etf_profile`, `price_daily`, and the catalog-derived overview do not
have a lookahead-bias issue: those frames are populated directly from
observed values without any retroactive multiplication.

## Logging

Every per-symbol oddity is recorded in two places:

1. The Python `logging` module, via the project-wide configuration in
   `maintainance_scripts.logging_setup`. Local runs print to stdout; Cloud
   Run runs serialise to JSON for Cloud Logging.
2. `transformation_report.parquet` at the destination root, schema:

| Column | Type | Description |
|--------|------|-------------|
| symbol | Utf8 | Ticker / pair / indicator |
| asset_type | Utf8 | stocks, etfs, forex, indices, cryptocurrencies, commodities, economic |
| frame | Utf8 | shareprice_daily, shareprice_intraday, price_daily, etf_profile, insider_df, sentiment_df, financials_quarterly, financials_annually |
| issue_type | Utf8 | see `ISSUE_TYPES` in `_common.py`. Includes dedup discrepancies, dedup drops, intraday orphans/nulls, schema cast failures, and the `financials_*` family (see "financials_quarterly, financials_annually" above). |
| count | UInt32 | Absolute count (rows or fields) |
| relative | Float32 | Count divided by row total or field total (null if not meaningful) |
| detail | Utf8 | Free-form payload (e.g. `"Close: 100.0 vs 100.4"`) |
| timestamp | Datetime | When the issue was recorded |

The report is **overwritten** each full run (it reflects the current
transformation, not a cumulative log) - same convention as
`ingestion_report.parquet`.

### Per-issue log levels

`transformation_report.parquet` always records the row. The Python
`logging` line that `TransformationReport.record` emits alongside the
row is demoted to `DEBUG` for issues that are structural / high-volume
and not individually actionable, and kept at `INFO` for everything else:

| Condition (any of these holds) | Console log level |
|--------------------------------|-------------------|
| `asset_type` in `{commodities, economic}` | DEBUG |
| `frame` in `{sentiment_df, insider_df}` | DEBUG |
| `issue_type == "financials_no_earnings_file"` | DEBUG |
| anything else | INFO |

Rationale:
- **commodities / economic** trip `dedup_dropped_null_row` on every US
  market holiday in the WTI / BRENT / NATURAL_GAS / TREASURY_YIELD_*
  series (AV emits the calendar date with a null value); the count is
  predictable and structural (~3-4% of rows over decades of history).
- **insider_df / sentiment_df** trip `dedup_value_discrepancy_over_1pct`
  in bulk because share counts and AV sentiment scores are routinely
  rewritten across snapshots; the per-row drift is reviewed in the
  parquet, not the console stream.
- **financials_no_earnings_file** fires once per stock that has no
  `earnings/SYMBOL_quarterly.parquet` anywhere (typically warrants,
  preferreds, and SPAC units). Both financials frames are saved as
  empty schema-only placeholders; nothing to act on at console-log time.

The parquet payload is unchanged in every case, so a downstream consumer
that wants the full picture reads the report directly rather than relying
on the console stream.

## CLI

```bash
# Default: read catalog/, historical/, daily/ from PROJECT_ROOT,
# write to <PROJECT_ROOT>/transformed/.
python data_transformation/transform.py

# Custom paths
python data_transformation/transform.py \
    --catalog-dir /path/to/catalog \
    --historical-dir /path/to/historical \
    --daily-dir /path/to/daily \
    --dest-dir /path/to/transformed

# Subset (skip finalize semantics not relevant here, but useful for partial
# rebuilds)
python data_transformation/transform.py --asset-types stocks etfs
python data_transformation/transform.py --symbols AAPL MSFT --asset-types stocks

# Wipe <dest>/stocks/ before processing for a clean slate. Useful when
# a new stock-only frame builder lands (e.g. financials_quarterly /
# financials_annually) and previously-saved stocks need to pick up the
# new frame, or to drop accumulated noise from earlier partial runs.
# Transformed data is regenerable from raw historical/ + daily/, so
# wiping it is cheap.
python data_transformation/transform.py --rebuild-stocks

# Skip the financials builder (Phase 6c) for stocks. Saves empty
# schema-only financials_quarterly.parquet / financials_annually.parquet
# placeholders. Useful for fast iteration during dev.
python data_transformation/transform.py --skip-financials
```

## Resume

A symbol is skipped on re-run if
`<dest>/<asset_type>/data_<SYMBOL>/metadata.json` already exists. To force
a re-transform, delete the symbol's folder (or the whole
`<dest>/<asset_type>/` tree). `assets_overview.parquet` and
`transformation_report.parquet` are always rewritten.

For stocks specifically, the `--rebuild-stocks` CLI flag wipes
`<dest>/stocks/` before processing - shorthand for the manual
deletion. Each per-symbol save is "all implemented frames present"
by design (see "Build invariant" below), so when a new stock-only
frame builder lands the existing symbol folders carry empty
placeholders for that frame and would otherwise be skipped silently.

### Build invariant

Each per-symbol folder is saved exactly once, after every implemented
frame for that asset type has been built in memory. A failure during
any phase aborts the whole symbol; no partial folder is written.
Consequence: when a new frame builder lands later, the existing
folders are still "complete" by the resume check's lights but lack
the new frame, so they need to be wiped to pick it up.

Coverage by asset type today:

| Asset type        | Frames built per symbol                                            |
|-------------------|--------------------------------------------------------------------|
| stocks            | shareprice_daily, shareprice_intraday, insider_df, sentiment_df, financials_quarterly, financials_annually |
| etfs              | shareprice_daily, shareprice_intraday, etf_profile                 |
| forex / indices / cryptocurrencies / commodities / economic | price_daily |

If a previously-saved stock predates the financials phase
(its `data_<SYM>/` was written before Phase 6c landed), run with
`--rebuild-stocks` once to backfill `financials_quarterly` and
`financials_annually` across every stock.

## Module structure

```
data_transformation/
├── __init__.py
├── README.md
├── AssetData.py
├── AssetDataService.py
├── AssetData_specifications.md
├── transform.py            # CLI orchestrator
├── _common.py              # source-file enumeration, sector lookup, transformation report
└── frames/
    ├── __init__.py
    ├── _dedup.py           # shared dedup-with-discrepancy-log helper
    ├── overview.py         # assets_overview.parquet (Phase 1)
    ├── price_daily.py      # price_daily for forex/indices/cryptocurrencies/
    │                       #  commodities/economic (Phase 2) +
    │                       #  build_shareprice_daily for stocks/etfs (Phase 3)
    ├── price_intraday.py   # build_shareprice_intraday for stocks/etfs (Phase 4)
    ├── etf_profile.py      # build_etf_profile for etfs (Phase 5)
    ├── insider.py          # build_insider_df for stocks (Phase 6a)
    ├── sentiment.py        # build_sentiment_df for stocks (Phase 6b)
    ├── financials.py       # build_financials for stocks (Phase 6c):
    │                       #  per-row PIT snapshot resolution,
    │                       #  report_table, quarterly + annual cell
    │                       #  mapping, annual-estimate extension
    └── stocks_etfs.py      # combined per-symbol orchestrator running
                            #  Phases 3, 4, 5, 6a, 6b, 6c in one pass so
                            #  the shareprice_daily.Date axis flows
                            #  directly into Phase 6c without
                            #  round-tripping through disk
```

The per-frame builders live under `frames/` to mirror the
`historical_data_setup/endpoints/` and `daily_data_service/endpoints/`
layout: one file per output frame, all sharing helpers from `_common.py`
and `frames/_dedup.py`, all driven by the orchestrator in `transform.py`.
Stocks and etfs additionally route through `frames/stocks_etfs.py`'s
combined orchestrator so the `shareprice_daily.Date` axis produced by
Phase 3 flows directly into Phase 6c without touching disk.

Tests live in `tests/data_transformation/` (the existing
`test_asset_data_service.py` covers the dataclasses themselves).

## Dependencies

- `polars` - all dataframe operations and parquet I/O
- `historical_data_setup._common` - `ASSET_TYPE_FILE_PREFIX`,
  `symbol_parquet_name` (single source of truth for per-symbol filenames;
  per [TODO.md](../TODO.md) these helpers may move to `maintainance_scripts/`
  later)
- `data_transformation.AssetData`, `data_transformation.AssetDataService`
  - dataclasses, `SCHEMAS`, `ASSET_LAYOUT`, `save_to`
- `maintainance_scripts.logging_setup` - logging configuration

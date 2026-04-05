import polars as pl
from dataclasses import dataclass
from typing import Dict

@dataclass
class AssetData:
    ###########
    # GENERAL #
    ###########
    ticker: str
    isin: str = ""

    # Stock, ETF, Commodity, Index, Economic
    asset_type: str = ""
    
    # Information about the asset in dict format
    about: str = ""

    # Sector of the asset
    sector: str = ""
    # 'other', 'industrials', 'healthcare', 'technology', 'financial-services', 'real-estate', 'energy', 'consumer-cyclical'


    ###########################
    # PRICES AND CORP-ACTIONS #
    ###########################
    shareprice_daily: pl.DataFrame = None
    # Columns 
    #  'Date'      : pl.Date
    #  'Open'      : pl.Float32
    #  'High'      : pl.Float32
    #  'Low'       : pl.Float32
    #  'Close'     : pl.Float32
    #  'AdjClose'  : pl.Float32
    #  'Volume'    : pl.Float32
    #  'Dividends' : pl.Float32
    #  'Splits'    : pl.Float32

    shareprice_intraday: pl.DataFrame = None
    # Columns 
    #  'Date'      : pl.Datetime
    #  'Open'      : pl.Float32
    #  'High'      : pl.Float32
    #  'Low'       : pl.Float32
    #  'Close'     : pl.Float32
    #  'AdjClose'  : pl.Float32
    #  'Volume'    : pl.Float32

    ##############
    # FINANCIALS #
    ##############
    financials_quarterly: pl.DataFrame = None 
    # Columns
    #  'fiscalDateEnding'             : pl.Date
    #  'reportedDate'                 : pl.Date
    #  'reportTime'                   : pl.String  ('pre-market', 'post-market')
    #  'reportedEPS'                  : pl.Float32
    #  'estimatedEPS'                 : pl.Float32
    #  'surprise'                     : pl.Float32
    #  'surprisePercentage'           : pl.Float32
    #  'grossProfit'                  : pl.Float32
    #  'totalRevenue'                 : pl.Float32
    #  'ebit'                         : pl.Float32
    #  'ebitda'                       : pl.Float32
    #  'totalAssets'                  : pl.Float32
    #  'totalCurrentLiabilities'      : pl.Float32
    #  'totalShareholderEquity'       : pl.Float32
    #  'commonStockSharesOutstanding' : pl.Float32
    #  'operatingCashflow'            : pl.Float32
    # todo consider additional, depending on how readily available there are.
    # maybe also company correction to old data
    
    financials_annually: pl.DataFrame = None
    # Columns
    #  'fiscalDateEnding'             : pl.Date
    #  'reportedEPS'                  : pl.Float32
    #  'grossProfit'                  : pl.Float32
    #  'totalRevenue'                 : pl.Float32
    #  'ebit'                         : pl.Float32
    #  'ebitda'                       : pl.Float32
    #  'totalAssets'                  : pl.Float32
    #  'totalCurrentLiabilities'      : pl.Float32
    #  'totalShareholderEquity'       : pl.Float32
    #  'operatingCashflow'            : pl.Float32
    # todo consider additional, depending on how readily available there are
    # maybe also company correction to old data

    #TODO
    #Insidertransaction data
    #Sentiment data
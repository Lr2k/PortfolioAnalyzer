from pathlib import Path
import tomllib
from datetime import date
from copy import deepcopy

import tomli_w

from PortfolioAnalyzer.core import CONFIG_PATH, CACHE_PATH
from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.asset_id import AssetIDs, StockID
from PortfolioAnalyzer.data.share import Currency, Category
import PortfolioAnalyzer.gateway.yf.api as yf
import PortfolioAnalyzer.gateway.toushinlib.web as toushin
from PortfolioAnalyzer.gateway.file.store import load_data

EXCHANGE_MAP_PATH = CONFIG_PATH / "exchange_map.toml"
PRICE_CACHE_PATH = CACHE_PATH / "prices.toml"

def collect_prices(asset_ids, target_date: date, no_cache: bool = False, cache_only: bool = False) -> tuple[Prices, AssetIDs]:
    match (no_cache, cache_only):
        case True, True:
            raise ValueError(f"矛盾する設定が指定されています。(no_chache:{no_cache}, cache_only:{cache_only})")
        case _, True:
            return pick_prices_from_cache(asset_ids, target_date)
        case True, _:
            fetched_prices, missing_ids = get_stock_prices(stock_ids,)
            collected_price_ls = list()
            missing_id_in_cache = list()
            if not no_cache:
                for a_id in asset_ids:
                    cached_p = cached_prices.get(a_id, target_date, excact_date=True)
                    if cached_p:
                        collected_price_ls.append(cached_p)
                    else:
                        missing_id_in_cache.append(a_id)
            else:
                missing_id_in_cache += asset_ids.records
            
            get_prices

def pick_prices_from_cache(asset_ids: AssetIDs, target_date: date) -> tuple[Prices, AssetIDs]:
    """
    キャッシュから価格情報を取得する。

    Parameters
    ----------
    asset_ids: AssetIDs
        価格情報を取得したい銘柄を格納したAssetIDs
    target_date: date
        価格情報を取得したい日付
    
    Returns
    -------
    Prices
        取得できた価格情報
    AssetIDs
        価格情報を取得できなかった銘柄を格納したAssetIDs
    """
    _, _, cached_prices, _, _ = load_data(PRICE_CACHE_PATH)
    price_record_ls = []
    missing_id_ls = []
    for a_id in asset_ids.records:
        match cached_prices.get(a_id, target_date, excact_date=True):
            case None:
                missing_id_ls.append(a_id)
            case price_record:
                price_record_ls.append(price_record)
    
    return Prices(records=price_record_ls), AssetIDs(records=missing_id_ls)

def get_prices(asset_ids: AssetIDs, target_date: date) -> tuple[Prices, AssetIDs]:
    stock_ids = asset_ids.get_by_category(Category.STOCK)
    stock_prices, missing_stocks = get_stock_prices(stock_ids, target_date)

def fetch_price_from_source(asset_ids: AssetIDs, target_date: date | None = None, last_data = True, save_cache=True) -> tuple[Prices, AssetIDs]:
    price_ls: list[PriceRecord] = list()
    missing_ids: list[AssetIDs] = list()

    # STOCK
    stock_ids = asset_ids.get_by_category(Category.STOCK)
    exc_ticker_map, exc_currency_map = try_get_exchange_map()
    fetched_stock_prices, missing_stocks = yf.get_stock_prices(
        stock_ids=stock_ids,
        end_date=target_date,
        exchange_map=exc_ticker_map,
        currency_map=exc_currency_map,
        period_date=7,
    )
    price_ls += [fetched_stock_prices.get(s_id, target_date, True) for s_id in stock_ids.records]

    # FUND
    fund_ids = asset_ids.get_by_category(Category.FUND)
    fund_name_code_map, missing_code_fund_ids, missing_code_map = toushin.get_fund_codes(fund_ids)
    toushin.get_fund_prices(fund_ids)




def _get_stock_prices(stock_ids: AssetIDs, target_date: date) -> tuple[Prices, AssetIDs]:
    exchange_map: dict
    try:
        exchange_map = tomllib.loads(EXCHANGE_MAP_PATH.read_text())                        
    except FileNotFoundError:
        exchange_map = {}

    exc_ticker_map = {exc: val["ticker"] for exc, val in exchange_map.items() if val["ticker"]}
    exc_currency_map = {exc: Currency(val["currency"]) for exc, val in exchange_map.items() if val["currency"]}

    stock_prices, map_missing_ids = get_stocks_price(stock_ids, target_date, exc_ticker_map, exc_currency_map)

    # MAPファイルの更新
    if len(map_missing_ids.records):
        new_exchange_map = {k: dict(v) for k, v in exchange_map.items()}
        for miss_id in map_missing_ids.records:
            new_exchange_map[miss_id.name]["ticker"] = None
            new_exchange_map[miss_id.name]["currency"] = None
        EXCHANGE_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        EXCHANGE_MAP_PATH.write_bytes(tomli_w.dumps(new_exchange_map).encode())
    
    return stock_prices, map_missing_ids
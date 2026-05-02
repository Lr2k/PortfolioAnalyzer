from datetime import datetime, date, timedelta, timezone

import yfinance as yf
from pandas import Timestamp
from pydantic import BaseModel, PrivateAttr, model_validator

from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.share import Currency, Category
from PortfolioAnalyzer.data.asset_id import AssetIDs, StockID
from PortfolioAnalyzer.data.rate import RateRecord, Rates


JST = timezone(timedelta(hours=9))

class RateRequest(BaseModel):
    '''
    get_rates に渡す通貨ペアリクエスト。

    (from_currency, to_currency) の順序に依存しない同一性を持つ。
    すなわち USD→JPY と JPY→USD は同一リクエストとみなされ、set による重複除去が行われる。

    Attributes
    ----------
    from_currency : Currency
        変換元通貨。
    to_currency : Currency
        変換先通貨。
    '''
    from_currency: Currency
    to_currency: Currency

    def ticker(self):
        return f'{self.from_currency}{self.to_currency}=X'

    def __hash__(self):
        return hash(frozenset({self.from_currency, self.to_currency}))

    def __eq__(self, other):
        if isinstance(other, RateRequest):
            return self.from_currency==other.from_currency and self.to_currency==other.from_currency
        else:
            return False

def _resolve_ticker(stock_id: StockID, exchange_map: dict[str, str], currency_map: dict[str, Currency]) -> tuple[str | None, Currency | None]:
    """
    株式銘柄に応じ、yfinanceでの問い合わせに利用できるティッカーと取引所で使用される通貨を解決する。
    マッピングが欠如している場合はNoneを返す。

    Return
    ------
    str or None
        yfinanceで使用できるティッカー
    Currency or None 
        取引所で使用される通貨。
    """
    exchange_ticker = exchange_map.get(stock_id.exchange)
    currency = currency_map.get(stock_id.exchange)

    ticker: str | None
    match exchange_ticker:
        case None:
            ticker = None
        case "":
            ticker = stock_id.ticker
        case x:
            ticker = f"{stock_id.ticker}.{x}"
    
    return ticker, currency

def _fetch_price_records(stock_id: StockID, ticker: str, currency: Currency, end_date: date, period_date: int) -> list[PriceRecord]:
    """
    １銘柄の株式の終値を取得する。データが取得できない場合は空のリストを返す。
    """
    yf_ticker = yf.Ticker(ticker)
    res_df_close = yf_ticker.history(
        period=f"{period_date}D",
        end=end_date + timedelta(days=1)
    )['Close'].dropna()
    if res_df_close is None or res_df_close.empty:
        return []
    else:
        res_min_date = res_df_close.index.min().date()
        return [
            PriceRecord(
                id=stock_id,
                date=target_date,
                price=float(res_df_close.asof(Timestamp(target_date.isoformat()).tz_localize(JST))),
                currency=currency,
            )
            for target_date in [end_date-td for td in map(timedelta, range(period_date))]
            if target_date >= res_min_date
        ]

def get_stock_prices(stock_ids: AssetIDs, end_date: date, exchange_map: dict[str, str], currency_map: dict[str, Currency], period_date: int = 7) -> tuple[Prices, AssetIDs, AssetIDs]:
    '''
    指定された日付、または指定された日からperiod_dateで指定された期間さかのぼって終値を返す。
    当日の株式価格は不確定の可能性があるため、実行日の前日までの指定を受け入れる。

    Parameters
    ----------
    stock_ids: AssetIDs
        価格を取得したい銘柄を格納したAssetIDs。
    end_date: date
        価格を取得した日付。
    exchange_map: dict[str, str]
        AssetIDsで使用されている取引所の名称とyfinanceで使用されるサフィックスの組み合わせ。(例: {"東証":"T", "NYSE": ""})
    currency_map: dict[str, str]
        取引所と通貨の組み合わせを指定する。(例: {"東証":Currency.JPY, "NYSE":Currency.USD})
    period_date: int, default is 7
        指定された日付の価格が取得できなかった場合に遡る日数。
    
    Returns
    -------
    Prices
        各銘柄の終値を収録した価格履歴。
    AssetIDs
        map_missing_ids, 取引所のサフィックスまたは通貨に関するマッピング情報が不足している銘柄。
    AssetIDs
        result_missing_ids, 価格情報を取得できなかった銘柄。


    Raises
    ------
    ValueError
        実行時以降の日付が指定されている場合。
    '''
    if end_date >= date.today():
        raise ValueError(f"end_dateには昨日までの日付を指定してください。(target_date:{target_date})")
    else:
        pass

    price_ls = []
    price_missing_id_ls = []
    map_missing_id_ls = []
    for s_id in stock_ids.records:
        if s_id.category == Category.STOCK:
            ticker, currency = _resolve_ticker(s_id, exchange_map, currency_map)
            if ticker and currency:
                match _fetch_price_records(s_id, ticker, currency, end_date, period_date):
                    case []:
                        price_missing_id_ls.append(s_id)
                    case ps:
                        price_ls += ps
            else:
                map_missing_id_ls.append(s_id)
        else:
            price_missing_id_ls.append(s_id)

    return Prices(records = price_ls), AssetIDs(records=map_missing_id_ls), AssetIDs(records=price_missing_id_ls)

def get_rates(from_currencies: list[Currency], to_currency: Currency, target_date: date) -> Rates:
    '''
    指定された日付、または指定された日からさかのぼって最新の為替レートを返す。

    Parameters
    ----------
    rate_requests : list[RateRequest]
        取得対象の通貨ペアリスト。重複は自動的に除去される。
    target_date : date
        基準日。指定日のデータが存在しない場合は最大3回（7日ずつ）遡って取得する。

    Returns
    -------
    Rates
        各通貨ペアのレートを収録した為替レート履歴。

    Raises
    ------
    ValueError
        最大3回（21日分）遡っても為替レートデータが取得できなかった場合。
    '''
    rq_list = [
        RateRequest(from_currency=from_currency, to_currency=to_currency)
        for from_currency in from_currencies
        if from_currency != to_currency
    ]
    yf_tickers = yf.Tickers(tickers=[rq.ticker() for rq in rq_list])

    retry_count = 0
    while retry_count < 3:
        res_df = yf_tickers.history(period='1W', end=target_date+timedelta(days=1), progress=True)
        if res_df.empty:
            target_date -= timedelta(days=7)
            retry_count += 1
            continue
        else:
            close_prices = res_df.xs('Close', axis=1, level=0)
            last_date = res_df.index.max()
            return Rates(
                records=[
                    RateRecord(
                        from_currency=rq.from_currency,
                        to_currency=rq.to_currency,
                        date=last_date.date(),
                        rate=float(close_prices.loc[last_date, rq.ticker()])
                    ) for rq in rq_list
                ]
            )
    else:
        currency_set_str = ",".join([f"{rq.to_currency}/{rq.from_currency}" for rq in rq_list])
        msg = f'{currency_set_str}について{target_date.strftime("%Y-%m-%d")}からさかのぼって21日間の情報を問い合わせましたが、情報を得られませんでした。'
        raise ValueError(msg)
        


if __name__ == '__main__':

    ids = AssetIDs(records = [
        StockID(name='大阪ガス', ticker='9533', exchange='東証', currency=Currency.JPY),
        StockID(name='メディカル・データ・ビジョン', ticker='3902',exchange='東証', currency=Currency.JPY),
        StockID(name='ジェクシード', exchange='東証', ticker='3719', currency=Currency.JPY),
        StockID(name='AT&T', exchange="NYSE", ticker='T', currency=Currency.USD),
    ])

    exchange_map = {
        "東証": "T",
        "NYSE": "",
    }

    currency_map = {
        "東証": Currency.JPY,
        "NYSE": Currency.USD,
    }

    target_date = date(year=2026, month=4, day=28)
    prices, map_mis, res_mis = get_stock_prices(ids, target_date, exchange_map, currency_map,24)
    print(prices)
    print(map_mis)
    print(res_mis)
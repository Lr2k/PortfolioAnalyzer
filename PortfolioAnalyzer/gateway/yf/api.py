from datetime import date, timedelta

import yfinance as yf
from pydantic import BaseModel, PrivateAttr, model_validator

from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.share import Currency
from PortfolioAnalyzer.data.asset_id import StockID
from PortfolioAnalyzer.data.rate import RateRecord, Rates


class StockRequest(BaseModel):
    '''
    get_stocks_price に渡す銘柄リクエスト。

    Attributes
    ----------
    ticker : str
        yfinance が扱う形式のティッカー（例: '9533.T', 'AAPL'）。
    name : str
        銘柄名。
    exchange : str
        取引所コード。(例: "T", "NYSE")
    currency : Currency
        価格の通貨。
    '''
    ticker: str
    name: str
    exchange: str
    currency: Currency

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

def build_stock_requests(stock_ids: list[StockID], exchange_map: dict[str, str], currency_map: dict[str, Currency]) -> tuple[list[StockRequest],list[StockID]]:
    '''
    StockID のリストから StockRequest のリストを構築する。

    exchange_map・currency_map に取引所名が見つからない銘柄は第2返り値に含める。

    Parameters
    ----------
    stock_ids : list[StockID]
        変換対象の銘柄識別情報リスト。
    exchange_map : dict[str, str]
        取引所名から yfinance のサフィックスへのマッピング。
        サフィックスが不要な取引所（例: NYSE）は空文字列 "" を指定する。
    currency_map : dict[str, Currency]
        取引所名から取引通貨へのマッピング。

    Returns
    -------
    requests : list[StockRequest]
        変換に成功した StockRequest のリスト。
    missing : list[StockID]
        exchange_map または currency_map に取引所名が見つからなかった StockID のリスト。
    '''
    sr_list: list[StockRequest] = list()
    missing_list: list[StockID] = list()
    for stock_id in stock_ids:
        if stock_id.exchange in exchange_map.keys() and stock_id.exchange in currency_map.keys():
            sr_list.append(StockRequest(
                ticker=stock_id.ticker if exchange_map[stock_id.exchange] == ""
                        else f"{stock_id.ticker}.{exchange_map[stock_id.exchange]}",
                name=stock_id.name,
                exchange=stock_id.exchange,
                currency=currency_map[stock_id.exchange]
            ))
        else:
            missing_list.append(stock_id)
    
    return sr_list, missing_list

def get_stocks_price(requests: list[StockRequest], target_date: date) -> Prices:
    '''
    指定された日付、または指定された日からさかのぼって最新の終値を返す。

    Parameters
    ----------
    requests : list[StockRequest]
        取得対象の銘柄リスト。
    target_date : date
        基準日。未来の日付を指定した場合は本日として扱う。
        指定日のデータが存在しない場合は最大3回（7日ずつ）遡って取得する。

    Returns
    -------
    Prices
        各銘柄の終値を収録した価格履歴。

    Raises
    ------
    ValueError
        最大3回（21日分）遡っても価格データが取得できなかった場合。
    '''
    tickers = [req.ticker for req in requests]
    id_map = {req.ticker: StockID(name=req.name, ticker=req.ticker, exchange=req.exchange) for req in requests}
    currency_map = {req.ticker: req.currency for req in requests}

    yf_tickers = yf.Tickers(tickers=tickers)
    retry_count = 0
    target_date = date.today() if target_date > date.today() else target_date

    while retry_count < 3:
        df = yf_tickers.history(period='1W', end=target_date + timedelta(days=1), progress=False)
        if df.empty:
            target_date -= timedelta(days=7)
            retry_count += 1
            continue
        else:
            close_prices = df.xs('Close', axis=1, level=0)
            last_date = df.index.max()
            retry_count += 1
            return Prices(
                records=[
                    PriceRecord(
                        id=id_map[ticker],
                        date=last_date.date(),
                        price=float(close_prices.loc[last_date, ticker]),
                        currency=currency_map[ticker],
                    )
                    for ticker in tickers
                ]
            )

    msg = f'{tickers}について{target_date.strftime("%Y-%m-%d")}からさかのぼって21日間の情報を問い合わせましたが、情報を得られませんでした。'
    raise ValueError(msg)

def get_rates(rate_requests: list[RateRequest], target_date: date) -> Rates:
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
    rq_list = list(set(rate_requests))
    yf_tickers = yf.Tickers(tickers=[rq.ticker() for rq in rq_list])

    retry_count = 0
    while retry_count < 3:
        res_df = yf_tickers.history(period='1W', end=target_date+timedelta(days=1), progress=False)
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
    currency_set_str = ",".join([f"{rq.to_currency}/{rq.from_currency}" for rq in rq_list])
    msg = f'{currency_set_str}について{target_date.strftime("%Y-%m-%d")}からさかのぼって21日間の情報を問い合わせましたが、情報を得られませんでした。'
    raise ValueError(msg)
        


if __name__ == '__main__':
    print(get_rates(rate_requests=[RateRequest(from_currency=Currency.USD, to_currency=Currency.JPY),], target_date=date.today()))

    stock_id_list = [
        StockID(name='大阪ガス', ticker='9533', exchange='東証', currency=Currency.JPY),
        StockID(name='メディカル・データ・ビジョン', ticker='3902',exchange='東証', currency=Currency.JPY),
        StockID(name='ジェクシード', exchange='東証', ticker='3719', currency=Currency.JPY),
        StockID(name='AT&T', exchange="NYSE", ticker='T', currency=Currency.USD),
    ]

    exchange_map = {
        "東証": "T",
        "NYSE": "",
    }

    currency_map = {
        "東証": Currency.JPY,
        "NYSE": Currency.USD,
    }

    target_date = date(year=2025, month=12, day=1)
    rq, miss = build_stock_requests(stock_id_list, exchange_map, currency_map)
    print(miss)

    prices = get_stocks_price(requests=rq, target_date=target_date)
    print(prices)
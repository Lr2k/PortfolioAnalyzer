import yfinance as yf
from pandas import Timestamp, Timedelta
from pydantic import BaseModel

from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.share import Category, Currency


class StockRequest(BaseModel):
    '''
    get_stocks_price に渡す銘柄リクエスト。

    Attributes
    ----------
    ticker : str
        yfinance が扱う形式のティッカー（例: '9533.T', 'AAPL'）。
    name : str
        銘柄名。
    currency : Currency
        価格の通貨。
    '''
    ticker: str
    name: str
    currency: Currency


def get_stocks_price(requests: list[StockRequest], date: Timestamp) -> Prices:
    '''
    指定された日付、または指定された日からさかのぼって最新の終値を返す。

    Parameters
    ----------
    requests : list[StockRequest]
        取得対象の銘柄リスト。
    date : Timestamp
        基準日。未来の日付を指定した場合は本日として扱う。
        指定日のデータが存在しない場合は最大3回（7日ずつ）遡って取得する。

    Returns
    -------
    Prices
        category=STOCK 固定。各銘柄の終値を収録した価格履歴。

    Raises
    ------
    ValueError
        最大3回（21日分）遡っても価格データが取得できなかった場合。
    '''
    tickers = [req.ticker for req in requests]
    name_map = {req.ticker: req.name for req in requests}
    currency_map = {req.ticker: req.currency for req in requests}

    yf_tickers = yf.Tickers(tickers=tickers)
    retry_count = 0
    date = Timestamp.today() if date > Timestamp.today() else date

    while retry_count < 3:
        df = yf_tickers.history(period='1w', end=date + Timedelta(days=1), progress=False)
        if df.empty:
            date -= Timedelta(days=7)
            retry_count += 1
            continue
        else:
            close_prices = df.xs('Close', axis=1, level=0)
            last_date = df.index.max()
            retry_count += 1
            return Prices(
                records=[
                    PriceRecord(
                        category=Category.STOCK,
                        name=name_map[ticker],
                        date=last_date.date(),
                        price=float(close_prices.loc[last_date, ticker]),
                        currency=currency_map[ticker],
                    )
                    for ticker in tickers
                ]
            )

    msg = f'{tickers}について{date.strftime("%Y-%m-%d")}からさかのぼって21日間の情報を問い合わせましたが、情報を得られませんでした。'
    raise ValueError(msg)


if __name__ == '__main__':
    requests = [
        StockRequest(ticker='9533.T', name='大阪ガス', currency=Currency.JPY),
        StockRequest(ticker='3902.T', name='メディカル・データ・ビジョン', currency=Currency.JPY),
        StockRequest(ticker='3719.T', name='ジェクシード', currency=Currency.JPY),
    ]
    date = Timestamp(year=2025, month=12, day=1)

    prices = get_stocks_price(requests=requests, date=date)
    print(prices)

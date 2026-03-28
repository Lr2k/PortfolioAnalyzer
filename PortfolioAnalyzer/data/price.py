from datetime import date

from pydantic import BaseModel, model_validator

from PortfolioAnalyzer.data.share import Currency
from PortfolioAnalyzer.data.asset_id import FundID, StockID, CryptoID, CashID

class PriceRecord(BaseModel):
    '''
    単一時点における銘柄の価格情報。

    Attributes
    ----------
    id : FundID | StockID | CryptoID | CashID
        銘柄の識別情報。
    date : date
        価格の基準日。
    price : float
        価格。
    currency : Currency
        価格の通貨。
    '''
    id: FundID | StockID | CryptoID | CashID
    date: date
    price: float
    currency: Currency

    def __str__(self):
        return f"{self.date}  {self.id.category}  {self.id.name}  {self.price} {self.currency}"

class Prices(BaseModel):
    '''
    複数銘柄・複数時点の価格情報を管理するクラス。

    (id, date) の組み合わせでユニークなレコードを管理する。

    Attributes
    ----------
    records : list[PriceRecord]
        価格レコードのリスト。

    Methods
    -------
    _deduplicate()
        初期化時にレコードの重複を除去する。
    '''
    records: list[PriceRecord]

    @model_validator(mode='after')
    def _deduplicate(self) -> 'Prices':
        '''
        初期化時にレコードの重複を除去する。

        (id, date) をキーとして重複を判定し、先着のレコードを残す。

        Returns
        -------
        Prices
            重複除去済みの Prices。
        '''
        self.records = list({(rec.id, rec.date): rec for rec in self.records}.values())

        return self

    def __str__(self):
        return '\n'.join(str(p) for p in self.records)

from datetime import date

from pydantic import BaseModel, model_validator, ConfigDict

from PortfolioAnalyzer.data.share import Currency
from PortfolioAnalyzer.data.asset_id import FundID, StockID, CryptoID, CashID


class ValuationRecord(BaseModel):
    '''
    単一時点における銘柄の評価額情報。

    Attributes
    ----------
    id : FundID | StockID | CryptoID | CashID
        銘柄の識別情報。
    value : float
        評価額。
    currency : Currency
        評価額の通貨。
    date : date
        評価額の基準日。
    '''
    model_config = ConfigDict(frozen=True)
    id: FundID | StockID | CryptoID | CashID
    value: float
    currency: Currency
    date: date

    def __str__(self):
        return f"{self.id.category}  {self.id.name}  {self.value} {self.currency}  {self.date}"


class Valuation(BaseModel):
    '''
    複数銘柄の評価額情報を管理するクラス。

    (id, date) の組み合わせでユニークなレコードを管理する。

    Attributes
    ----------
    records : list[ValuationRecord]
        評価額レコードのリスト。
    timestamp : date | None
        評価額の基準日。不明または空の場合は None。

    Methods
    -------
    _deduplicate()
        初期化時にレコードの重複を除去する。
    '''
    records: list[ValuationRecord]
    timestamp: date | None

    @model_validator(mode='after')
    def _deduplicate(self) -> 'Valuation':
        '''
        初期化時にレコードの重複を除去する。

        (id, date) をキーとして重複を判定し、先着のレコードを残す。

        Returns
        -------
        Valuation
            重複除去済みの Valuation。
        '''
        self.records = list({
            (rec.id, rec.date): rec
            for rec in self.records
        }.values())

        return self
    
    def __str__(self):
        lines = [f"timestamp:{self.timestamp}",]
        for rec in self.records:
            lines.append(rec.__str__())
        
        return "\n".join(lines)
        

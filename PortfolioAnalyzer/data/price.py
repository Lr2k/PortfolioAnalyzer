from datetime import date

from pydantic import BaseModel, model_validator, PrivateAttr, ConfigDict

from PortfolioAnalyzer.data.share import Currency
from PortfolioAnalyzer.data.asset_id import FundID, StockID, CryptoID, CashID

class PriceRecord(BaseModel):
    '''
    単一時点における銘柄の価格情報。イミュータブル。

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
    model_config = ConfigDict(frozen=True)
    id: FundID | StockID | CryptoID | CashID
    date: date
    price: float
    currency: Currency

    def __hash__(self):
        return hash((self.id, self.date))

    def __eq__(self, other):
        if isinstance(other, PriceRecord):
            return self.date == other.date and self.id == other.id
        else:
            return False
    
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
    get(id, target_date)
        指定銘柄・日付の価格情報を返す。
    '''
    records: list[PriceRecord] = list()
    _records_index: dict[FundID | StockID | CryptoID | CashID, dict[date, PriceRecord]] = PrivateAttr(default_factory=dict)

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
        self.records = list(set(self.records))
        for rec in self.records:
            if rec.id in self._records_index.keys():
                self._records_index[rec.id][rec.date] = rec
            else:
                self._records_index[rec.id] = {rec.date: rec}

        return self

    def get(self, id, target_date:date | None = None) -> PriceRecord | None:
        '''
        指定銘柄・日付の価格情報を返す。

        日付が完全一致しない場合は target_date 以前で最新のレコードを使用する。

        Parameters
        ----------
        id : FundID | StockID | CryptoID | CashID
            取得対象の銘柄識別情報。
        target_date : date | None, optional
            取得対象の日付。None の場合は最新のレコードを返す。

        Returns
        -------
        PriceRecord | None
            見つかった場合は PriceRecord、銘柄が存在しないまたは
            target_date 以前のデータがない場合は None。
        '''
        date_rec = self._records_index.get(id)
        if date_rec is None:
            return None
        else:
            match [dt for dt in date_rec.keys() if target_date is None or dt <= target_date]:
                case []:
                    return None
                case dts:
                    return date_rec[max(dts)]

    def __str__(self):
        return '\n'.join(str(p) for p in self.records)
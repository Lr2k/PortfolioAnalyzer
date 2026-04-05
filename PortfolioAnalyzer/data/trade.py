from enum import StrEnum
from datetime import date

from pydantic import BaseModel, ConfigDict

from PortfolioAnalyzer.data.share import Currency
from PortfolioAnalyzer.data.asset import (
    AssetRecord,
    Assets,
    merge_assets,
)
from PortfolioAnalyzer.data.asset_id import (
    FundID,
    StockID,
    CryptoID,
    CashID,
)

class TradeAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

class TradeRecord(BaseModel):
    '''
    単体の取引履歴。

    Attributes
    ----------
    date : date
        取引の行われた日付。
    action : TradeAction
        売りまたは買い。(BUY / SELL)
    id : FundID | StockID | CryptoID | CashID
        取引した銘柄の識別情報。
    price : float
        取引単価。
    currency : Currency
        取引通貨。
    amount : float
        取引数量。
    '''
    model_config = ConfigDict(frozen=True)
    date: date
    action: TradeAction
    id: FundID | StockID | CryptoID | CashID
    price: float
    currency: Currency
    amount: float

    def __str__(self):
        return f"{self.date.strftime('%Y/%m/%d')}  {self.id.name}  {self.id.category}  {self.action}  {self.amount}  {self.price} {self.currency}"

class TradeHistory(BaseModel):
    '''
    複数の取引履歴を管理するクラス。

    Attributes
    ----------
    records : list[TradeRecord]
        取引レコードのリスト。

    Methods
    -------
    to_assets(past_assets, from_date, to_date)
        取引履歴から保有資産スナップショットを生成する。
    '''
    records: list[TradeRecord]

    def __str__(self):
        return '\n'.join(str(record) for record in self.records)

    def to_assets(self, past_assets: Assets | None = None, from_date: date | None = None, to_date: date | None = None) -> Assets:
        '''
        取引履歴から保有資産スナップショットを生成する。

        Parameters
        ----------
        past_assets : Assets | None, optional
            過去の保有資産。指定した場合、取引履歴から生成した資産と結合する。
        from_date : date | None, optional
            集計開始日（以降）。Noneの場合は制限なし。
        to_date : date | None, optional
            集計終了日（以前）。Noneの場合は制限なし。timestampにも使用される。

        Returns
        -------
        Assets
            取引履歴を反映した保有資産スナップショット。
        '''
        filtered_records = [
            record
            for record in self.records
            if ((from_date is None) or record.date >= from_date)
            and ((to_date is None) or record.date <= to_date)
        ]

        new_timestamp: date | None
        if to_date is None:
            new_timestamp = max(r.date for r in filtered_records) if filtered_records else None
        else:
            new_timestamp = to_date
        
        new_assets = Assets(
            records = [
                AssetRecord(
                    id = trade_record.id,
                    amount = trade_record.amount 
                        if trade_record.action == TradeAction.BUY
                        else -trade_record.amount,
                )
                for trade_record in filtered_records
            ],
            timestamp=new_timestamp
        )
        
        return new_assets if past_assets is None else merge_assets(past_assets, new_assets)


def merge_trade_history(*trade: TradeHistory) -> TradeHistory:
    '''
    複数の TradeHistory のレコードを結合して返す。

    Parameters
    ----------
    *trade : TradeHistory
        結合する取引履歴。

    Returns
    -------
    TradeHistory
        結合された取引履歴。
    '''
    new_records: list[TradeRecord] = list()
    for t in trade:
        new_records += t.records

    return TradeHistory(
        records=new_records,
    )
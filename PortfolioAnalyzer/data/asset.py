from datetime import date
from collections import defaultdict

from pydantic import BaseModel, model_validator, ConfigDict

from PortfolioAnalyzer.data.asset_id import AssetID, FundID, StockID, CryptoID, CashID


class AssetRecord(BaseModel):
    '''
    単体の保有資産残高。

    Attributes
    ----------
    id : FundID | StockID | CryptoID | CashID
        銘柄の識別情報。
    amount : float
        保有数量。
    '''
    model_config = ConfigDict(frozen=True)
    id: FundID | StockID | CryptoID | CashID
    amount: float

    def __str__(self):
        return f"{self.id.name}  {self.id.category}  {self.amount}"

class Assets(BaseModel):
    '''
    特定時点における保有資産の残高スナップショット。

    Attributes
    ----------
    records : list[AssetRecord]
        保有資産のレコードリスト。
    timestamp : date | None
        残高の基準日。不明または空の場合は None。

    Methods
    -------
    _aggregate()
        初期化時に同一 ID の amount を集約し、amount=0 のレコードを除外する。
    '''
    records: list[AssetRecord]
    timestamp: date | None = None

    @model_validator(mode='after')
    def _aggregate(self) -> 'Assets':
        '''
        初期化時に同一 ID の amount を集約し、amount=0 のレコードを除外する。

        同一 ID のレコードが複数存在する場合、amount を合計して一つにまとめる。
        合計 amount が 0 のレコードは除外される。

        Returns
        -------
        Assets
            集約済みの Assets。
        '''
        amount_by_id: dict[AssetID, float] = defaultdict(float)

        for record in self.records:
            amount_by_id[record.id] += record.amount
        
        self.records = [
            AssetRecord(
                id=id,
                amount=amount,
            )
            for id, amount in amount_by_id.items()
            if amount != 0
        ]

        return self

    def __str__(self):
        lines = [f"timestamp: {self.timestamp}"]
        lines += [str(asset) for asset in self.records]
        return '\n'.join(lines)

def merge_assets(*assets: Assets, timestamp: date | None = None) -> Assets:
    '''
    複数のAssetsのレコードを結合して返す。

    Parameters
    ----------
    *assets : Assets
        結合する保有資産。
    timestamp : date | None, optional
        新たな基準日。Noneの場合は各assetsのtimestampのうち最新の非None値を採用する。全てNoneの場合はNone。

    Returns
    -------
    Assets
        結合された保有資産。
    '''
    new_timestamp: date | None
    if timestamp is None:
        timestamps = [asset.timestamp for asset in assets if asset.timestamp is not None]
        new_timestamp = max(timestamps) if timestamps else None
    else:
        new_timestamp = timestamp
    
    new_records = list()
    for asset in assets:
        new_records += asset.records
    
    return Assets(
        records=new_records,
        timestamp=new_timestamp,
    )
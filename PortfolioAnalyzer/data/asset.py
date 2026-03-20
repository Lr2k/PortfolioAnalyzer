from enum import StrEnum
from datetime import date
from collections import defaultdict

from pydantic import BaseModel, model_validator

from PortfolioAnalyzer.data.share import Category


class FundID(BaseModel):
    '''
    投資信託の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    '''
    name: str

    def __str__(self):
        return self.name

class StockID(BaseModel):
    '''
    株式の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    ticker : str
        ティッカーシンボル。
    exchange : str
        取引所コード。(例: "T", "NYSE")
    '''
    name: str
    ticker: str
    exchange: str

    def __str__(self):
        return f"{self.exchange} {self.ticker}  {self.name}"

class CryptoID(BaseModel):
    '''
    仮想通貨の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    symbol : str
        通貨シンボル。(例: "BTC", "ETH")
    '''
    name: str
    symbol: str

    def __str__(self):
        return f"{self.symbol}  {self.name}"

class CashID(BaseModel):
    '''
    現金の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    symbol : str
        通貨シンボル。(例: "JPY", "USD")
    '''
    name: str
    symbol: str

    def __str__(self):
        return f"{self.symbol}  {self.name}"

class AssetIDs(BaseModel):
    '''
    資産種別ごとの識別情報を管理するクラス。

    Attributes
    ----------
    funds : list[FundID]
        投資信託の識別情報リスト。
    stocks : list[StockID]
        株式の識別情報リスト。
    cryptos : list[CryptoID]
        仮想通貨の識別情報リスト。
    cashes : list[CashID]
        現金の識別情報リスト。

    Methods
    -------
    _deduplicate()
        初期化時に各リストの重複を除去する。
    get_id(category, name)
        カテゴリと銘柄名から識別情報を返す。
    '''
    funds: list[FundID] = []
    stocks: list[StockID] = []
    cryptos: list[CryptoID] = []
    cashes: list[CashID] = []

    @staticmethod
    def _count_name(records: list, name: str) -> int:
        '''
        リスト内で指定した銘柄名を持つレコードの数を返す。

        Parameters
        ----------
        records : list
            検索対象のレコードリスト。
        name : str
            検索する銘柄名。

        Returns
        -------
        int
            一致するレコードの数。
        '''
        num = 0
        for record in records:
            if record.name == name:
                num += 1
            else:
                pass
        return num

    @staticmethod
    def _pick_name(records: list, name: str) -> FundID | StockID | CryptoID | CashID:
        '''
        リスト内から指定した銘柄名を持つ最初のレコードを返す。

        Parameters
        ----------
        records : list
            検索対象のレコードリスト。
        name : str
            検索する銘柄名。

        Returns
        -------
        FundID | StockID | CryptoID | CashID
            一致した識別情報レコード。

        Raises
        ------
        KeyError
            指定した銘柄名が存在しない場合。
        '''
        for record in records:
            if record.name == name:
                return record
            else:
                continue
        raise KeyError(f"recordsに{name}が存在しません。")
    
    @model_validator(mode='after')
    def _deduplicate(self) -> 'AssetIDs':
        '''
        初期化時に各リストの重複を除去する。

        各資産種別ともカテゴリ内で name をキーとして重複を判定し、先着のレコードを残す。

        Returns
        -------
        AssetIDs
            重複除去済みのAssetIDs。
        '''
        self.funds   = list({f_id.name: f_id for f_id in self.funds}.values())
        self.stocks  = list({s_id.name: s_id for s_id in self.stocks}.values())
        self.cryptos = list({cr_id.name: cr_id for cr_id in self.cryptos}.values())
        self.cashes  = list({ca_id.name: ca_id for ca_id in self.cashes}.values())

        return self

    def get_id(self, category: Category, name: str) -> FundID | StockID | CryptoID | CashID:
        '''
        カテゴリと銘柄名から識別情報を返す。

        Parameters
        ----------
        category : Category
            資産種別。
        name : str
            銘柄名。

        Returns
        -------
        FundID | StockID | CryptoID | CashID
            指定した銘柄の識別情報。

        Raises
        ------
        KeyError
            指定した銘柄名が存在しない場合。
        ValueError
            同一銘柄名が複数存在する場合、またはcategoryが不正な場合。
        '''
        match category:
            case Category.FUND:
                return self._get_fund_id(name)
            case Category.STOCK:
                return self._get_stock_id(name)
            case Category.CRYPT:
                return self._get_crypto_id(name)
            case Category.CASH:
                return self._get_cash_id(name)
            case _:
                raise ValueError(f"category引数には{(cat for cat in Category)}のいずれかを指定してください。(category: {category})")

    def _get_fund_id(self, name: str) -> FundID:
        match self._count_name(self.funds, name):
            case 0:
                raise KeyError(f"FundIDに{name}は存在しません。")
            case 1:
                return self._pick_name(self.funds, name)
            case x:
                raise ValueError(f"{x}個のFundIDが存在します。")

    def _get_stock_id(self, name: str) -> StockID:
        match self._count_name(self.stocks, name):
            case 0:
                raise KeyError(f"StockIDに{name}は存在しません。")
            case 1:
                return self._pick_name(self.stocks, name)
            case x:
                raise ValueError(f"{x}個のStockIDが存在します。")

    def _get_crypto_id(self, name: str) -> CryptoID:
        match self._count_name(self.cryptos, name):
            case 0:
                raise KeyError(f"CryptoIDに{name}は存在しません。")
            case 1:
                return self._pick_name(self.cryptos, name)
            case x:
                raise ValueError(f"{x}個のCryptoIDが存在します。")

    def _get_cash_id(self, name: str) -> CashID:
        match self._count_name(self.cashes, name):
            case 0:
                raise KeyError(f"CashIDに{name}は存在しません。")
            case 1:
                return self._pick_name(self.cashes, name)
            case x:
                raise ValueError(f"{x}個のCashIDが存在します。")

    def __str__(self):
        lines = []
        lines.append("FundID:")
        lines += [f"  {f}" for f in self.funds]
        lines.append("StockID:")
        lines += [f"  {s}" for s in self.stocks]
        lines.append("CryptoID:")
        lines += [f"  {c}" for c in self.cryptos]
        lines.append("CashID:")
        lines += [f"  {c}" for c in self.cashes]
        return '\n'.join(lines)

class AssetRecord(BaseModel):
    '''
    単体の保有資産残高を管理するクラス。

    Attributes
    ----------
    name : str
        銘柄名。
    category : Category
        資産種別。
    amount : float
        保有数量。
    '''
    name: str
    category: Category
    amount: float

    def __str__(self):
        return f"{self.name}  {self.category}  {self.amount}"

class Assets(BaseModel):
    '''
    特定時点における保有資産の残高スナップショット。

    Attributes
    ----------
    records : list[AssetRecord]
        保有資産のレコードリスト。
    timestamp : date | None
        残高の基準日。不明または空の場合はNone。

    Methods
    -------
    _aggregate()
        初期化時に同名銘柄のamountを集約し、amount=0のレコードを除外する。
    '''
    records: list[AssetRecord]
    timestamp: date | None = None

    @model_validator(mode='after')
    def _aggregate(self) -> 'Assets':
        '''
        初期化時に同名銘柄のamountを集約し、amount=0のレコードを除外する。

        同一銘柄名が複数のレコードに存在する場合、amountを合計して一つにまとめる。
        合計amountが0のレコードは除外される。

        Returns
        -------
        Assets
            集約済みのAssets。
        '''
        amount_by_name: dict[str, float] = defaultdict(float)
        category_by_name: dict[str, Category] = dict()

        for record in self.records:
            amount_by_name[record.name] += record.amount
            category_by_name[record.name] = record.category
        
        self.records = [
            AssetRecord(
                name=name,
                amount=amount_by_name[name],
                category=category
            )
            for name, category in category_by_name.items()
            if amount_by_name[name] != 0
        ]

        return self

    def __str__(self):
        lines = [f"timestamp: {self.timestamp}"]
        lines += [str(record) for record in self.records]
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
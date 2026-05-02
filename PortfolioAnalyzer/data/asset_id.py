from typing import ClassVar

from pydantic import BaseModel, model_validator, PrivateAttr, ConfigDict

from PortfolioAnalyzer.data.share import Category

class AssetID(BaseModel):
    '''
    全資産 ID の基底クラス。

    Attributes
    ----------
    name : str
        銘柄名。
    '''
    model_config = ConfigDict(frozen=True)
    name: str

    def __hash__(self):
        return hash((self.category, self.name))
    
    def __eq__(self, other):
        if not isinstance(other, AssetID):
            return False
        else:
            return self.category == other.category and self.name == other.name

class FundID(AssetID):
    '''
    投資信託の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    category : Category
        資産種別。常に ``Category.FUND``。
    '''
    category: ClassVar[Category] = Category.FUND

    def __str__(self):
        return f"{self.category} {self.name}"

class StockID(AssetID):
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
    category : Category
        資産種別。常に ``Category.STOCK``。
    '''
    category: ClassVar[Category] = Category.STOCK

    ticker: str
    exchange: str

    def __str__(self):
        return f"{self.category} {self.exchange} {self.ticker}  {self.name}"

class CryptoID(AssetID):
    '''
    仮想通貨の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    symbol : str
        通貨シンボル。(例: "BTC", "ETH")
    category : Category
        資産種別。常に ``Category.CRYPT``。
    '''
    category: ClassVar[Category] = Category.CRYPT

    symbol: str

    def __str__(self):
        return f"{self.category} {self.symbol} {self.name}"

class CashID(AssetID):
    '''
    現金の識別情報。

    Attributes
    ----------
    name : str
        銘柄名。
    symbol : str
        通貨シンボル。(例: "JPY", "USD")
    category : Category
        資産種別。常に ``Category.CASH``。
    '''
    category: ClassVar[Category] = Category.CASH

    symbol: str

    def __str__(self):
        return f"{self.category} {self.symbol} {self.name}"

class AssetIDs(BaseModel):
    '''
    資産識別情報を管理するクラス。

    Attributes
    ----------
    records : list[AssetID]
        全資産の識別情報リスト。重複は初期化時に除去される。

    Methods
    -------
    get_id(category, name)
        カテゴリと銘柄名から識別情報を返す。
    get_by_category(category)
        指定カテゴリの識別情報リストを返す。
    '''
    records: list[AssetID] = list()
    _record_index: dict[tuple[Category, str], AssetID] = PrivateAttr(default_factory=dict)

    @model_validator(mode='after')
    def _build(self) -> 'AssetIDs':
        '''
        初期化時に重複除去とインデックス構築を行う。

        ``(category, name)`` をキーとして重複を判定し、先着のレコードを残す。
        その後、同キーで ``_record_index`` を構築する。

        Returns
        -------
        AssetIDs
            重複除去済みの AssetIDs。
        '''
        self.records = list(
            {
                (id.category, id.name): id
                for id in self.records
            }.values()
        )

        self._record_index = {
            (id.category, id.name): id
            for id in self.records
        }

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
            指定した銘柄情報が存在しない場合。
        '''
        target = self._record_index.get((category, name))
        if target is None:
            raise KeyError(f"AssetIDが存在しません。(category:{category}, name:{name})")
        else:
            return target

    def get_by_category(self, category: Category) -> 'AssetIDs':
        '''
        指定カテゴリの識別情報を返す。

        Parameters
        ----------
        category : Category
            資産種別。

        Returns
        -------
        AssetIDs
            指定カテゴリの識別情報。該当なしの場合は空の AssetIDs。
        '''
        return AssetIDs(
            records = list(
                rec for key, rec in self._record_index.items()
                if key[0] == category
            )
        )
        
        

    def __str__(self):
        lines = [str(asset_id) for asset_id in self.records]
        return '\n'.join(lines)


def merge_asset_ids(*asset_ids: AssetIDs) -> AssetIDs:
    '''
    複数の AssetIDs を結合する。

    Parameters
    ----------
    *asset_ids : AssetIDs
        結合する AssetIDs。

    Returns
    -------
    AssetIDs
        全レコードを結合した新しい AssetIDs。
    '''
    new_records: list[AssetID] = list()
    for ids in asset_ids:
        new_records += ids.records

    return AssetIDs(records=new_records)
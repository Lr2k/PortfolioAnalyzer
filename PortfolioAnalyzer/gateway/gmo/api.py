from pathlib import Path
import hmac, hashlib
import  time
from datetime import datetime
from configparser import (
    ConfigParser,
    MissingSectionHeaderError
)

import requests as rq

from PortfolioAnalyzer.data.asset import (
    AssetRecord,
    Assets,
    AssetIDs,
    CryptoID,
    CashID,
)
from PortfolioAnalyzer.data.share import (
    Category,
)

PUBLIC_API_URL = "https://api.coin.z.com/public"
PRIVATE_API_URL = "https://api.coin.z.com/private"

class ApiKeyConfigError(Exception):
    '''GMOコインAPIキーの設定ファイルが不正な場合に発生する例外。'''
    def __init__(self, filepath=None):
        super().__init__(self)
        self.filepath = filepath if filepath is not None else ""

    def __str__(self):
        return "\n".join([
            "APIキーの設定ファイルに誤りが含まれます。以下のように記述してください。",
            f"(filepath: {self.filepath})",
            "",
            "[KEYS]",
            "API-KEY:<APIキー>",
            "SEC-KEY:<シークレットキー>",
        ])

class NoApiKeyError(Exception):
    '''APIキーが未設定の状態でプライベートAPIを呼び出した場合に発生する例外。'''
    def __str__(self,):
        return "ApiHandlerにapiキーが設定されていません。"

class ApiHandler(object):
    '''
    GMOコインAPIとのHTTP通信を管理するクラス。

    Attributes
    ----------
    api_key : str | None
        GMOコインのAPIキー。
    secret_key : str | None
        GMOコインのシークレットキー。
    apikey_available : bool
        APIキーとシークレットキーが両方設定されているかどうか。

    Methods
    -------
    get(path, private, headers)
        GMOコインAPIにGETリクエストを送信する。
    get_rate(symbol)
        指定した暗号資産シンボルの最新レートを取得する。
    get_assets(asset_name_map)
        GMOコインAPIから保有資産の残高と識別情報を取得する。
    '''
    def __init__(self, apikey_filepath: Path | None,  api_key: str | None = None, secret_key: str | None = None):
        '''
        APIキーを初期化する。

        ファイルパスを指定した場合はファイルからAPIキーを読み込む。
        Noneの場合はapi_keyとsecret_keyを直接指定する。

        Parameters
        ----------
        apikey_filepath : Path | None
            APIキーが記述された設定ファイルのパス。以下の形式で記述する。

            [KEYS]
            API-KEY:<APIキー>
            SEC-KEY:<シークレットキー>
        api_key : str | None, optional
            GMOコインのAPIキー。apikey_filepathがNoneの場合に使用される。
        secret_key : str | None, optional
            GMOコインのシークレットキー。apikey_filepathがNoneの場合に使用される。

        Raises
        ------
        ApiKeyConfigError
            設定ファイルの形式が不正な場合。
        '''
        if apikey_filepath is not None:
            config = ConfigParser()

            with apikey_filepath.open() as f:
                try:
                    config.read_file(f)
                except MissingSectionHeaderError:
                    raise ApiKeyConfigError(filepath=apikey_filepath.as_posix())

                if (
                    "KEYS" in config.keys() and\
                    (
                        ("API-KEY" in config["KEYS"].keys()) and\
                        ("SEC-KEY" in config["KEYS"].keys())
                    )
                ):
                    self.api_key = config["KEYS"]["API-KEY"]
                    self.secret_key = config["KEYS"]["SEC-KEY"]
                else:
                    raise ApiKeyConfigError(filepath=apikey_filepath.as_posix())
        else:
            self.api_key = api_key
            self.secret_key = secret_key
        self.apikey_available = ((self.api_key is not None ) and (self.secret_key is not None))

    def get(self, path: str, private = False, headers:dict | None = None ) -> dict:
        '''
        GMOコインAPIにGETリクエストを送信する。

        Parameters
        ----------
        path : str
            APIエンドポイントのパス。(例: "/v1/ticker?symbol=BTC")
        private : bool, optional
            Trueの場合はプライベートAPIを使用する。APIキーが必要。
        headers : dict | None, optional
            リクエストヘッダー。プライベートAPIの認証情報などを含む。

        Returns
        -------
        dict
            APIレスポンスのJSON。

        Raises
        ------
        NoApiKeyError
            private=TrueかつAPIキーが未設定の場合。
        '''
        if private and not self.apikey_available:
            raise NoApiKeyError()
        else:
            pass

        end_point = PRIVATE_API_URL if private else PUBLIC_API_URL
        return rq.get(
            url=f"{end_point}{path}",
            headers=headers,
        ).json()

    def get_rate(self, symbol: str) -> int:
        '''
        指定した暗号資産シンボルの最新レートを取得する。

        Parameters
        ----------
        symbol : str
            暗号資産のシンボル。(例: "BTC", "ETH")

        Returns
        -------
        int
            最新レート（円）。
        '''
        response_dict = self.get(
            path= f"/v1/ticker?symbol={symbol}",
            private=False,
        )
        return int(response_dict["data"][0]["last"])

    def get_assets(self, asset_name_map: dict[str, str]) -> tuple[Assets, AssetIDs, set[str]]:
        '''
        GMOコインAPIから保有資産の残高と識別情報を取得する。

        asset_name_mapに未登録のシンボルは "UnknownCrypto{シンボル}" という名前で
        Assetsに含め、unknown_symbolsとして返す。
        unknown_symbolsはasset_name_mapに追加すべきシンボルを把握するために使用できる。

        Parameters
        ----------
        asset_name_map : dict[str, str]
            シンボルから銘柄名へのマッピング。(例: {"BTC": "Bitcoin", "JPY": "日本円"})

        Returns
        -------
        assets : Assets
            現時点での保有資産の残高スナップショット。
        ids : AssetIDs
            保有資産の識別情報。
        unknown_symbols : set[str]
            asset_name_mapに未登録だったシンボルの集合。
        '''
        now_time = datetime.now()
        timestamp = '{0}000'.format(int(time.mktime(now_time.timetuple())))
        method = 'GET'
        path = '/v1/account/assets'
        text = ''.join([timestamp, method, path])

        sign = hmac.new(
            key=bytes(self.secret_key.encode('ascii')),
            msg=bytes(text.encode('ascii')),
            digestmod=hashlib.sha256
        ).hexdigest()

        headers = {
            "API-KEY" : self.api_key,
            "API-TIMESTAMP" : timestamp,
            "API-SIGN" : sign,
        }

        response_data = self.get(path=path, private=True, headers=headers)['data']

        jpy_amount: float = 0.0
        sym_amo_pairs: list[tuple[str, float]] = list()
        for asset in response_data:
            match asset['symbol'], float(asset['amount']):
                case 'JPY', amount:
                    jpy_amount = amount
                case _, 0.0:
                    pass
                case symbol, amount:
                    sym_amo_pairs.append((symbol, amount))

        unknown_symbols = set(sym for sym, _ in sym_amo_pairs) - set(asset_name_map.keys())

        name_sym_amo: list[tuple[str, str, float]] = [
            (f'UnknownCrypto{sym}' if sym in unknown_symbols else asset_name_map[sym], sym, amo)
            for sym, amo in sym_amo_pairs
        ]

        jpy_name = asset_name_map.get('JPY', 'UnknownCashJPY')

        assets = Assets(
            records=[
                AssetRecord(name=name, category=Category.CRYPT, amount=amo)
                for name, _, amo in name_sym_amo
            ] + [
                AssetRecord(name=jpy_name, category=Category.CASH, amount=jpy_amount)
            ],
            timestamp=now_time.date(),
        )

        ids = AssetIDs(
            cryptos=[CryptoID(name=name, symbol=sym) for name, sym, _ in name_sym_amo],
            cashes=[CashID(name=jpy_name, symbol='JPY')],
        )

        return assets, ids, unknown_symbols

if __name__=="__main__":
    crypto_name_map = {"BTC": "Bitcoin", "JPY": "日本円", "BCH": "BitcoinCash"}
    handler = ApiHandler(apikey_filepath=Path("/home/user0/.cred/gmo/apikey"))
    assets, ids, unknown_sym = handler.get_assets(crypto_name_map)
    print(ids)
    print(assets)
    print(unknown_sym)
    for crypto_id in ids.cryptos:
        print(f"{crypto_id.symbol}:{handler.get_rate(crypto_id.symbol)}JPY")

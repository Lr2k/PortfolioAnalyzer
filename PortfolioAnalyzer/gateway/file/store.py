from pathlib import Path
import json

from PortfolioAnalyzer.data.share import Category, Currency
from PortfolioAnalyzer.data.asset_id import AssetIDs, AssetID, FundID, StockID, CryptoID, CashID
from PortfolioAnalyzer.data.asset import Assets, AssetRecord
from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.rate import Rates, RateRecord
from PortfolioAnalyzer.data.trade import TradeHistory, TradeRecord

def save_data(
    path: Path,
    asset_ids: AssetIDs | None = None,
    assets_ls: list[Assets] | None = None,
    prices: Prices | None = None,
    rates: Rates | None = None,
    trade_history: TradeHistory | None = None,
) -> None:
    """ポートフォリオデータを JSON ファイルに保存する。

    `assets_ls`・`prices`・`trade_history` の各レコードが参照する `AssetID` は
    自動的に収集され、`asset_ids` で明示的に渡したものと合わせて
    ``"AssetIDs"`` セクションに書き出される。
    `Assets` のみ複数インスタンス（異なる timestamp）を渡せる。
    その他のデータクラスは 1 インスタンスずつ。

    Parameters
    ----------
    path : Path
        書き出し先の JSON ファイルパス。既存ファイルは上書きされる。
    asset_ids : AssetIDs, optional
        明示的に保存したい `AssetID` のコレクション。
        `assets_ls`・`prices`・`trade_history` から自動収集されるものと
        マージされる。`None` の場合は自動収集分のみ保存される。
    assets_ls : list[Assets], optional
        timestamp の異なる複数の保有残高スナップショット。
        `None` の場合は ``"AssetsLS"`` セクションを出力しない。
    prices : Prices, optional
        各銘柄の価格レコード群。
        `None` の場合は ``"Prices"`` セクションを出力しない。
    rates : Rates, optional
        為替レートレコード群。
        `None` の場合は ``"Rates"`` セクションを出力しない。
    trade_history : TradeHistory, optional
        売買履歴レコード群。
        `None` の場合は ``"TradeHistory"`` セクションを出力しない。

    Returns
    -------
    None

    Notes
    -----
    `AssetID` のサブクラス判別に必要な ``category`` フィールドは Pydantic の
    `ClassVar` であるため `model_dump_json()` では出力されない。そのため
    シリアライズはすべて手組みの `_*_to_dict` 関数で行い、``category`` を
    明示的に辞書に含めている。
    """
    id_from_assets: list[AssetID]
    if assets_ls is not None:
        id_from_assets = []
        for asset_ls in [a.records for a in assets_ls]:
            id_from_assets += [a.id for a in asset_ls]
    else:
        id_from_assets = []
    
    id_from_prices: list[AssetID]
    if prices is not None:
        id_from_prices = [p.id for p in prices.records]
    else:
        id_from_prices = []
    
    id_from_trade_history: list[AssetID]
    if trade_history is not None:
        id_from_trade_history = [t.id for t in trade_history.records]
    else:
        id_from_trade_history = []
    
    asset_ids_rec_none_enable = [] if asset_ids is None else asset_ids.records

    total_asset_ids = AssetIDs(
        records=\
            asset_ids_rec_none_enable +\
            id_from_assets +\
            id_from_prices +\
            id_from_trade_history
    )

    data_dict = {
        "AssetIDs": _ids_to_dict(total_asset_ids) if len(total_asset_ids.records) != 0 else None,
        "AssetsLS": [_assets_to_dict(assets) for assets in assets_ls] if assets_ls is not None else None,
        "Prices": _prices_to_dict(prices) if prices is not None else None,
        "Rates": _rates_to_dict(rates) if rates is not None else None,
        "TradeHistory": _trade_history_to_dict(trade_history) if trade_history is not None else None,
    }

    path.write_text(json.dumps(data_dict, indent=2, ensure_ascii=False))

def load_data(path: Path) -> tuple[AssetIDs | None, list[Assets] | None, Prices | None, Rates | None, TradeHistory | None]:
    """JSON ファイルからポートフォリオデータを読み込む。

    ``save_data`` で書き出したファイルを復元する。
    JSON の各セクションが ``null`` の場合、対応する戻り値も ``None`` になる。
    ``AssetIDs`` セクションが ``null`` の場合、それに依存する
    ``AssetsLS`` と ``Prices`` も ``None`` を返す。

    Parameters
    ----------
    path : Path
        読み込む JSON ファイルのパス。

    Returns
    -------
    asset_ids : AssetIDs or None
        銘柄識別情報のコレクション。
    assets_ls : list[Assets] or None
        保有残高スナップショットのリスト。``asset_ids`` が ``None`` の場合も ``None``。
    prices : Prices or None
        価格レコード群。``asset_ids`` が ``None`` の場合も ``None``。
    rates : Rates or None
        為替レートレコード群。
    trade_history : TradeHistory or None
        売買履歴レコード群。

    Raises
    ------
    FileNotFoundError
        ``path`` が存在しない場合。
    KeyError
        JSON 内の ``AssetIDs`` に存在しない銘柄を他セクションが参照している場合。
    """
    data_dict = json.loads(path.read_text())

    asset_ids = _dict_to_ids(data_dict["AssetIDs"])

    if asset_ids is not None:
        assets_ls = _dict_to_assets_ls(data_dict["AssetsLS"], asset_ids)
        prices = _dict_to_prices(data_dict["Prices"], asset_ids)
    else:
        assets_ls = None
        prices = None
    
    rates = _dict_to_rates(data_dict["Rates"])
    trade_history = _dict_to_trade_history(data_dict["TradeHistory"], asset_ids)

    return asset_ids, assets_ls, prices, rates, trade_history

def _id_to_dict(asset_id: AssetID) -> dict:
    match asset_id:
        case FundID():
            return {"name":asset_id.name, "category": asset_id.category}
        case StockID():
            return {"name":asset_id.name, "ticker":asset_id.ticker, "exchange":asset_id.exchange, "category":asset_id.category}
        case CryptoID():
            return {"name":asset_id.name, "symbol":asset_id.symbol, "category":asset_id.category}
        case CashID():
            return {"name":asset_id.name, "symbol":asset_id.symbol, "category":asset_id.category}

def _dict_to_id(id_dict: dict) -> AssetID:
    match id_dict["category"]:
        case Category.FUND:
            return FundID(name=id_dict["name"])
        case Category.STOCK:
            return StockID(name=id_dict["name"], ticker=id_dict["ticker"], exchange=id_dict["exchange"])
        case Category.CRYPT:
            return CryptoID(name=id_dict["name"], symbol=id_dict["symbol"])
        case Category.CASH:
            return CashID(name=id_dict["name"], symbol=id_dict["symbol"])

def _ids_to_dict(asset_ids: AssetIDs) -> dict:
    return {
        "records": [_id_to_dict(ar) for ar in asset_ids.records]
    }

def _dict_to_ids(ids_dict: dict | None) -> AssetIDs | None:
    if ids_dict is not None:
        return AssetIDs(
            records = [_dict_to_id(id_dict) for id_dict in ids_dict["records"]]
        )
    else:
        return None

def _asset_record_to_dict(asset: AssetRecord) -> dict:
    return {
        "id": {
            "name": asset.id.name,
            "category": asset.id.category,
        },
        "amount": asset.amount,
    }

def _dict_to_asset_record(asset_dict: dict, asset_ids: AssetIDs) -> AssetRecord:
    return AssetRecord(
        id=asset_ids.get_id(
            category=Category(asset_dict["id"]["category"]),
            name=asset_dict["id"]["name"],
        ),
        amount=asset_dict["amount"],
    )

def _assets_to_dict(assets: Assets) -> dict:
    return {
        "records": [_asset_record_to_dict(a) for a in assets.records],
        "timestamp": str(assets.timestamp) if assets.timestamp is not None else None,
    }

def _dict_to_assets_ls(assets_dict_ls: list[dict] | None, asset_ids: AssetIDs) -> list[Assets] | None:
    if assets_dict_ls is not None:
        return [
            Assets(
                records = [
                    _dict_to_asset_record(asset_dict, asset_ids)
                    for asset_dict in assets_dict["records"]
                ],
                timestamp=date.fromisoformat(assets_dict["timestamp"])
                            if assets_dict["timestamp"] is not None else None,
            )
            for assets_dict in assets_dict_ls
        ]
    else:
        return None

def _price_record_to_dict(price: PriceRecord) -> dict:
    return {
        "id": {
            "name": price.id.name,
            "category": price.id.category,
        },
        "date": str(price.date),
        "price": price.price,
        "currency": price.currency,
    }

def _dict_to_price_record(price_dict: dict, asset_ids: AssetIDs) -> PriceRecord:
    return PriceRecord(
        id=asset_ids.get_id(
            category=Category(price_dict["id"]["category"]),
            name=price_dict["id"]["name"],
        ),
        price=float(price_dict["price"]),
        date=date.fromisoformat(price_dict["date"]) if price_dict["date"] is not None else None,
        currency=Currency(price_dict["currency"])
    )

def _prices_to_dict(prices: Prices) -> dict:
    return {
        "records": [_price_record_to_dict(pr) for pr in prices.records],
    }

def _dict_to_prices(prices_dict: dict | None, asset_ids: AssetIDs) -> Prices | None:
    if prices_dict is not None:
        return Prices(
            records=[
                _dict_to_price_record(price_dict, asset_ids)
                for price_dict in prices_dict["records"]
            ]
        )
    else:
        return None

def _rate_record_to_dict(rate_rec: RateRecord) -> dict:
    return {
        "from_currency": rate_rec.from_currency,
        "to_currency": rate_rec.to_currency,
        "date": str(rate_rec.date),
        "rate": rate_rec.rate,
    }

def _dict_to_rate_record(rate_dict: dict) -> RateRecord:
    return RateRecord(
        from_currency=Currency(rate_dict["from_currency"]),
        to_currency=Currency(rate_dict["to_currency"]),
        date=date.fromisoformat(rate_dict["date"]) if rate_dict["date"] is not None else None,
        rate=float(rate_dict["rate"]),
    )

def _rates_to_dict(rates: Rates) -> dict:
    return {
        "records": [_rate_record_to_dict(rr) for rr in rates.records],
    }

def _dict_to_rates(rates_dict: dict | None) -> Rates | None:
    if rates_dict is not None:
        return Rates(
            records=[
                _dict_to_rate_record(rate_dict)
                for rate_dict in rates_dict["records"]
            ]
        )
    else:
        return None

def _trade_record_to_dict(trade_rec: TradeRecord) -> dict:
    return {
        "id": {
            "name": trade_rec.id.name,
            "category": trade_rec.id.category,
        },
        "date": str(trade_rec.date),
        "action": trade_rec.action,
        "price": trade_rec.price,
        "currency": trade_rec.currency,
        "amount": trade_rec.amount,
    }

def _dict_to_trade_record(trade_dict: dict, asset_ids: AssetIDs) -> TradeRecord:
    return TradeRecord(
        date=date.fromisoformat(trade_dict["date"]) if trade_dict["date"] is not None else None,
        action = TradeAction(trade_dict["action"]),
        id=asset_ids.get_id(Category(trade_dict["id"]["category"]), trade_dict["id"]["name"]),
        price=float(trade_dict["price"]),
        currency=Currency(trade_dict["currency"]),
        amount=float(trade_dict["amount"]),
    )

def _trade_history_to_dict(trade_his: TradeHistory) -> dict:
    return {
        "records": [_trade_record_to_dict(tr) for tr in trade_his.records]
    }

def _dict_to_trade_history(trade_history_dict:dict | None, asset_ids: AssetIDs) -> TradeHistory | None:
    if trade_history_dict is not None:
        return TradeHistory(
            records=[
                _dict_to_trade_record(trade_dict, asset_ids)
                for trade_dict in trade_history_dict["records"]
            ]
        )
    else:
        return None

if __name__=="__main__":
    from datetime import date
    from PortfolioAnalyzer.data.share import Category, Currency
    from PortfolioAnalyzer.data.trade import TradeAction

    path = Path("./data.json")

    fund_a = FundID(name="eMAXIS Slim 全世界株式")
    fund_b = FundID(name="eMAXIS Slim 米国株式")
    stock_a = StockID(name="トヨタ自動車", ticker="7203", exchange="T")
    stock_b = StockID(name="Apple", ticker="AAPL", exchange="NASDAQ")
    crypto_a = CryptoID(name="ビットコイン", symbol="BTC")
    crypto_b = CryptoID(name="イーサリアム", symbol="ETH")
    cash_a = CashID(name="日本円", symbol="JPY")
    cash_b = CashID(name="米ドル", symbol="USD")

    assets1 = Assets(
        records=[
            AssetRecord(id=fund_a, amount=100.0),
            AssetRecord(id=stock_a, amount=50.0),
            AssetRecord(id=crypto_a, amount=0.5),
        ],
        timestamp=date(2024, 3, 31),
    )
    assets2 = Assets(
        records=[
            AssetRecord(id=fund_b, amount=200.0),
            AssetRecord(id=stock_b, amount=10.0),
            AssetRecord(id=cash_a, amount=500000.0),
        ],
        timestamp=None,
    )

    prices = Prices(
        records=[
            PriceRecord(id=fund_a, date=date(2024, 3, 31), price=20000.0, currency=Currency.JPY),
            PriceRecord(id=stock_a, date=date(2024, 3, 29), price=3500.0, currency=Currency.JPY),
            PriceRecord(id=crypto_a, date=date(2024, 3, 31), price=10000000.0, currency=Currency.JPY),
        ]
    )

    rates = Rates(
        records=[
            RateRecord(from_currency=Currency.USD, to_currency=Currency.JPY, date=date(2024, 3, 31), rate=151.5),
            RateRecord(from_currency=Currency.USD, to_currency=Currency.JPY, date=date(2024, 2, 29), rate=149.8),
            RateRecord(from_currency=Currency.USD, to_currency=Currency.JPY, date=date(2024, 1, 31), rate=146.2),
        ]
    )

    trade_history = TradeHistory(
        records=[
            TradeRecord(date=date(2024, 1, 15), action=TradeAction.BUY, id=fund_a, price=18000.0, currency=Currency.JPY, amount=100.0),
            TradeRecord(date=date(2024, 2, 20), action=TradeAction.BUY, id=stock_b, price=185.0, currency=Currency.USD, amount=10.0),
            TradeRecord(date=date(2024, 3, 10), action=TradeAction.BUY, id=crypto_b, price=3500.0, currency=Currency.USD, amount=2.0),
        ]
    )

    asset_ids = AssetIDs(
        records=[cash_b, crypto_b]
    )

    save_data(
        path=path,
        #asset_ids=asset_ids,
        #assets_ls=[assets1, assets2],
        #prices=prices,
        #rates=rates,
        trade_history=trade_history,
    )

    loaded_ids, loaded_assets_ls, loaded_prices, loaded_rates, loaded_trade_history = load_data(path)

    print(loaded_ids)
    if loaded_assets_ls is None:
        print(loaded_assets_ls)
    else:
        for loaded_assets in loaded_assets_ls:
            print(loaded_assets)
    print(loaded_prices)
    print(loaded_rates)
    print(loaded_trade_history)
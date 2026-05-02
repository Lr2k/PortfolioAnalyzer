from pathlib import Path
import tomllib

import tomli_w

from PortfolioAnalyzer.data.share import Currency

def try_get_exchange_map(exchange_map_path: Path) -> tuple[dict[str, str], dict[str, str]]:
    exchange_map: dict[str, str]
    try:
        exchange_map = tomllib.loads(exchange_map_path.read_text())                        
    except FileNotFoundError:
        exchange_map = dict()
    print(exchange_map)
    exc_ticker_map = {exc: val["ticker"] for exc, val in exchange_map.items() if val["ticker"] and val["ticker"] != "TODO"}
    exc_currency_map = {exc: Currency(val["currency"]) for exc, val in exchange_map.items() if val["currency"] and val["currency"] != "TODO"}

    return exc_ticker_map, exc_currency_map

def fill_missing_entries(exchange_map_path: Path, exchanges: list[str], refresh: bool = False):
    """
    取引所に関するマッピング情報を保持するファイルに未登録の取引所を追加する。
    ファイルが存在しない場合は新規に作成する。

    Parametrs
    ---------
    exchange_map_path: Path
        ファイルパス
    excahnges: list[str]
        マッピングにエントリーを登録する取引所
    refresh: bool, default is False
        Trueの場合、既存の値をNoneで上書きする。
    """
    exist_map: dict[str, str]
    try:
        exist_map = tomllib.loads(exchange_map_path.read_text())                        
    except FileNotFoundError:
        exist_map = {}

    # 重複する場合は右が優先される。
    new_map: dict[str, dict[str, str]]
    if refresh:
        new_map = exist_map | {exchange:{"ticker":"TODO", "currency": "TODO"} for exchange in exchanges}
    else:
        new_map = {exchange:{"ticker":"TODO", "currency": "TODO"} for exchange in exchanges} | exist_map

    with exchange_map_path.open("wb") as f:
        tomli_w.dump(new_map, f)


if __name__=="__main__":
    fill_missing_entries(Path("./exchange_map_test.toml"), ["東証", "NYSE", "NYSE ARCA"], False)
    print(try_get_exchange_map(Path("./exchange_map_test.toml")))
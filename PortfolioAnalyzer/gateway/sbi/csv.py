from enum import StrEnum

from pandas import (
    DataFrame,
    Series,
    NA,
    Float64Dtype,
    StringDtype,

    read_csv,
    to_datetime,
)

from PortfolioAnalyzer.data.trade import (
    TradeAction,
    TradeRecord,
    TradeHistory,
    Currency,
    merge_trade_history,
)
from PortfolioAnalyzer.data.share import Category
from PortfolioAnalyzer.data.asset_id import (
    AssetIDs,
    FundID,
    StockID,
)


__all__ = (
    "AccountType",
    "load_sbi_csv",
    "parse_jpy_trade_history",
    "parse_non_jpy_trade_history",
)


class AccountType(StrEnum):
    '''SBI証券の口座種別。JPY（円建て）またはNONJPY（外貨建て）。'''
    JPY = "JPY"
    NONJPY = "NONJPY"


def _parse_jpy_trade_type(trade_type_series: Series[str]) -> tuple[Series[TradeAction], Series[Category]]:
    '''
    取り込んだ円建口座約定履歴の"取引"の情報をparseする。

    Parameters
    ----------
    trade_type_series : Series[str]
        "取引"の項のみを抽出したSeries。

    Returns
    -------
    action : Series[TradeAction]
        TradeAction.BUY、TradeAction.SELLのいずれか。
    category : Series[Category]
        Category.FUND、Category.STOCKのいずれか。
    '''
    action_mapping = {
        "投信金額買付": TradeAction.BUY,
        "株式現物買": TradeAction.BUY,
        "投信金額解約": TradeAction.SELL,
        "株式現物売": TradeAction.SELL,
    }
    category_mapping = {
        "投信金額買付": Category.FUND,
        "株式現物買": Category.STOCK,
        "投信金額解約": Category.FUND,
        "株式現物売": Category.STOCK,
    }

    action = trade_type_series.map(action_mapping)
    category = trade_type_series.map(category_mapping)

    return action, category

def _parse_price_jpy(price_series: Series[int | float], category_series: Series[Category]) -> Series[float]:
    '''
    円建口座約定履歴の約定単価を計算する。
    とくに、投資信託の約定単価を約定口数1当たりの単価に修正する。

    Parameters
    ----------
    price_series : Series[int | float]
    category_series : Series[Category]
        dtypeはCategorical

    Returns
    -------
    price_series : Series[float]
    '''
    new_price_series = Series(NA, index=price_series.index, dtype=Float64Dtype())
    new_price_series[category_series==Category.STOCK] = price_series[category_series==Category.STOCK]
    new_price_series[category_series==Category.FUND] = price_series[category_series==Category.FUND] / 10000

    return new_price_series

def parse_jpy_trade_history(trade_history: DataFrame) -> tuple[TradeHistory, AssetIDs]:
    '''
    円建口座取引履歴を解釈(parse)する。

    Parameters
    ----------
    trade_history : DataFrame
        SBIの円建口座約定履歴のCSVを読み込んだDataFrame。

    Returns
    -------
    trade_history : TradeHistory
        パース済みの取引履歴。
    asset_ids : AssetIDs
        取引履歴から抽出した資産識別情報。
    '''

    action, category = _parse_jpy_trade_type(trade_type_series=trade_history.loc[:, "取引"])

    names = trade_history.loc[:, "銘柄"].astype(StringDtype()).str.strip()
    prices = _parse_price_jpy(
        price_series=trade_history.loc[:, "約定単価"].astype(dtype=Float64Dtype()),
        category_series=category,
    )
    amounts = trade_history.loc[:, "約定数量"].astype(dtype=Float64Dtype())
    dates = to_datetime(trade_history.loc[:, "約定日"])

    tickers = trade_history.loc[:, "銘柄コード"].astype(StringDtype()).str.strip()
    exchanges = trade_history.loc[:, "市場"].astype(StringDtype()).str.strip()

    fund_mask = category == Category.FUND
    stock_mask = category == Category.STOCK

    def make_id(n, c, t, e):
        if c == Category.FUND:
            return FundID(name=n)
        else:
            return StockID(name=n, ticker=t, exchange=e)

    parsed_trade_history = TradeHistory(
        records=[
            TradeRecord(
                date=d.date(),
                currency=Currency.JPY,
                action=a,
                id=make_id(n, c, t, e),
                price=p,
                amount=amt,
            )
            for d, a, n, c, t, e, p, amt in zip(dates, action, names, category, tickers, exchanges, prices, amounts)
        ]
    )

    ids = AssetIDs(
        records=(
            [FundID(name=n) for n in names[fund_mask]] +
            [
                StockID(name=n, ticker=t, exchange=e)
                for n, t, e in zip(
                    names[stock_mask],
                    tickers[stock_mask],
                    exchanges[stock_mask],
                )
            ]
        ),
    )

    return parsed_trade_history, ids


def _parse_non_jpy_stock_id(names: Series[str]) -> tuple[Series[str], Series[str], Series[str]]:
    '''
    外貨建口座約定履歴のCSVに記載されている銘柄名から銘柄名のみとティッカー、取引所名を抽出する。

    Parameters
    ----------
    names : Series[str]

    Returns
    -------
    names : Series[str]
        銘柄名
    tickers : Series[str]
        ティッカー
    exchanges : Series[str]
        取引所
    '''
    left_exchange = names.str.split("/", n=1, expand=True)
    name_ticker = left_exchange[0].str.strip().str.rsplit(' ', n=1, expand=True)
    return (
        name_ticker[0].str.strip().astype(dtype=StringDtype()),
        name_ticker[1].str.strip().astype(dtype=StringDtype()),
        left_exchange[1].str.strip().astype(dtype=StringDtype()),
    )

def parse_non_jpy_trade_history(trade_history: DataFrame) -> tuple[TradeHistory, AssetIDs]:
    '''
    外貨建口座取引履歴をparseする。

    Parameters
    ----------
    trade_history : DataFrame
        SBIの外貨建口座約定履歴のCSVを読み込んだDataFrame。

    Returns
    -------
    trade_history : TradeHistory
        パース済みの取引履歴。
    asset_ids : AssetIDs
        取引履歴から抽出した資産識別情報。
    '''
    currency_mapping = {
        "日本円" : Currency.JPY,
        "米国ドル" : Currency.USD,
    }
    action_mapping = {
        "買付" : TradeAction.BUY,
        "売却" : TradeAction.SELL,
    }

    names, tickers, exchanges = _parse_non_jpy_stock_id(trade_history.loc[:, "銘柄名"].astype(dtype=StringDtype()))
    amounts = trade_history.loc[:, "約定数量"].astype(dtype=Float64Dtype())
    total_price = trade_history.loc[:, "受渡金額"].astype(dtype=Float64Dtype())
    dates = to_datetime(trade_history.loc[:, "国内約定日"], format="%Y年%m月%d日")
    currencies = trade_history.loc[:, "通貨"].map(currency_mapping)
    actions = trade_history.loc[:, "取引"].map(action_mapping)
    prices = total_price / amounts

    parsed_trade_history = TradeHistory(
        records=[
            TradeRecord(
                date=d.date(),
                currency=cur,
                action=act,
                id=StockID(name=n, ticker=t, exchange=e),
                price=p,
                amount=amt,
            )
            for d, cur, act, n, t, e, p, amt in zip(dates, currencies, actions, names, tickers, exchanges, prices, amounts)
        ]
    )

    ids = AssetIDs(
        records=[StockID(name=n, ticker=t, exchange=e) for n, t, e in zip(names, tickers, exchanges)],
    )

    return parsed_trade_history, ids

def load_sbi_csv(filepath, account_type: AccountType) -> DataFrame:
    '''
    SBI証券の約定履歴CSVを読み込む。

    Parameters
    ----------
    filepath : str | Path
        CSVファイルのパス。
    account_type : AccountType
        口座種別。AccountType.JPY（円建て）またはAccountType.NONJPY（外貨建て）。

    Returns
    -------
    DataFrame
        読み込んだCSVのDataFrame。
    '''
    match account_type:
        case AccountType.JPY:
            return read_csv(
                filepath_or_buffer=filepath,
                header=4,
                skip_blank_lines=True,
                encoding="shift_jis",
                dtype=StringDtype(),
            )
        case AccountType.NONJPY:
            return read_csv(
                filepath_or_buffer=filepath,
                header=2,
                skip_blank_lines=True,
                encoding="shift_jis",
                dtype=StringDtype(),
            )
        case _:
            raise ValueError(f"引数に適切な通貨を指定してください。(account_type:{account_type}, \"JPY\"または\"NONJPY\")")

if __name__=="__main__":
    jpy_filepath = "./test/data/sbi_csv/SBI_約定履歴_円建て_dummy1_shiftjis.csv"
    nonjpy_filepath = "./test/data/sbi_csv/SBI_約定履歴_外貨建て_dummy1_shiftjis.csv"

    jpy_trade_history, jpy_ids = parse_jpy_trade_history(
        load_sbi_csv(filepath=jpy_filepath, account_type=AccountType.JPY)
    )

    nonjpy_trade_history, non_jpy_ids = parse_non_jpy_trade_history(
        load_sbi_csv(filepath=nonjpy_filepath, account_type=AccountType.NONJPY)
    )

    ids = AssetIDs(
        records=jpy_ids.records + non_jpy_ids.records,
    )
    history = merge_trade_history(jpy_trade_history, nonjpy_trade_history)

    print(ids)
    print(history)

    ast = history.to_assets()

    print(ast)
from pathlib import Path
from io import StringIO

from pandas import (
    Timestamp,
    StringDtype,
    NA,
    read_csv,
)

from ...utils.df import assert_dataframe_equal
from PortfolioAnalyzer.gateway.sbi.csv import (
    parse_jpy_trade_history,
    parse_non_jpy_trade_history,
    load_sbi_csv,
    AccountType,
)
from PortfolioAnalyzer.data.trade import (
    TradeHistoryKey,
    TradeAction,
    Currency,
    Category
)


def write_dummy_sbi_csv(path: Path, header_line: int, lines: list[str]) -> None:
    full_lines = ["dummy",] * header_line + lines
    
    path.write_text(
        data="\n".join(full_lines),
        encoding="shift_jis",
    )

def test_load_sbi_csv_jpy(tmp_path: Path) -> None:
    csv_path = tmp_path / "jpy_dummy.csv"
    lines = [
        "col1,col2",
        "壱,弐",
        "1,2",
    ]

    write_dummy_sbi_csv(
        path=csv_path,
        header_line=4,
        lines=lines,
    )

    df = load_sbi_csv(
        filepath=csv_path,
        account_type=AccountType.JPY,
    )

    assert list(df.columns) == ["col1", "col2"]
    assert df.shape == (2,2)
    assert df.iloc[0]["col1"] == "壱"
    assert df.iloc[0]["col2"] == "弐"
    assert df.iloc[1]["col1"] == "1"
    assert df.iloc[1]["col2"] == "2"

def test_load_sbi_csv_nonjpy(tmp_path: Path) -> None:
    csv_path = tmp_path / "nonjpy_dummy.csv"
    lines = [
        "col1,col2",
        "壱,弐",
        "1,2",
    ]

    write_dummy_sbi_csv(
        path=csv_path,
        header_line=2,
        lines=lines,
    )

    df = load_sbi_csv(
        filepath=csv_path,
        account_type=AccountType.NONJPY,
    )

    assert list(df.columns) == ["col1", "col2"]
    assert df.shape == (2,2)
    assert df.iloc[0]["col1"] == "壱"
    assert df.iloc[0]["col2"] == "弐"
    assert df.iloc[1]["col1"] == "1"
    assert df.iloc[1]["col2"] == "2"

def test_parse_jpy_trade_history(tmp_path: Path):
    text = "\n".join((
        '約定日,銘柄,銘柄コード,市場,取引,期限,預り,課税,約定数量,約定単価,手数料/諸経費等,税額,受渡日,受渡金額/決済損益',
        '"2000/01/01","ファンドA",,,投信金額買付,"--"," NISA(成) ","--",2000,15000,--,--,"2000/01/03",3000',
        '"2000/02/01","ファンドA",,,投信金額解約,"--"," NISA(成) ","--",2000,10000,--,--,"2000/02/03",2000',
        '"2000/09/01","株式1","1234","東証",株式現物買,"--"," NISA(成) ","--",10,3000,--,--,"2000/09/03",30000',
        '"2000/10/01","株式1","1234","東証",株式現物売,"--"," NISA(成) ","--",10,2000,--,--,"2000/10/03",20000',
    ))
    df = read_csv(
        filepath_or_buffer=StringIO(text),
        skip_blank_lines=True,
        dtype=StringDtype(),
    )
    trade_jpy = parse_jpy_trade_history(df)

    assert_dataframe_equal(
        df=trade_jpy,
        expected_columns=list(TradeHistoryKey),
        expected_dtypes={
            TradeHistoryKey.DATE: "datetime64[ns]",
            TradeHistoryKey.CURRENCY: "category",
            TradeHistoryKey.ACTION: "category",
            TradeHistoryKey.EXCHANGE: "category",
            TradeHistoryKey.TICKER: "category",
            TradeHistoryKey.NAME: "category",
            TradeHistoryKey.CATEGORY: "category",
            TradeHistoryKey.PRICE: "Float64",
            TradeHistoryKey.QUANTITY: "Int64",
        },
        expected_rows=[
            {
                TradeHistoryKey.DATE: Timestamp("2000-01-01"),
                TradeHistoryKey.CURRENCY: Currency.JPY,
                TradeHistoryKey.ACTION: TradeAction.BUY,
                TradeHistoryKey.EXCHANGE: NA,
                TradeHistoryKey.TICKER: NA,
                TradeHistoryKey.NAME: "ファンドA",
                TradeHistoryKey.CATEGORY: Category.FUND,
                TradeHistoryKey.PRICE: 1.5,
                TradeHistoryKey.QUANTITY: 2000,
            },
            {
                TradeHistoryKey.DATE: Timestamp("2000-02-01"),
                TradeHistoryKey.CURRENCY: Currency.JPY,
                TradeHistoryKey.ACTION: TradeAction.SELL,
                TradeHistoryKey.EXCHANGE: NA,
                TradeHistoryKey.TICKER: NA,
                TradeHistoryKey.NAME: "ファンドA",
                TradeHistoryKey.CATEGORY: Category.FUND,
                TradeHistoryKey.PRICE: 1,
                TradeHistoryKey.QUANTITY: 2000,
            },
            {
                TradeHistoryKey.DATE: Timestamp("2000-09-01"),
                TradeHistoryKey.CURRENCY: Currency.JPY,
                TradeHistoryKey.ACTION: TradeAction.BUY,
                TradeHistoryKey.EXCHANGE: "東証",
                TradeHistoryKey.TICKER: "1234",
                TradeHistoryKey.NAME: "株式1",
                TradeHistoryKey.CATEGORY: Category.STOCK,
                TradeHistoryKey.PRICE: 3000,
                TradeHistoryKey.QUANTITY: 10,
            },
            {
                TradeHistoryKey.DATE: Timestamp("2000-10-01"),
                TradeHistoryKey.CURRENCY: Currency.JPY,
                TradeHistoryKey.ACTION: TradeAction.SELL,
                TradeHistoryKey.EXCHANGE: "東証",
                TradeHistoryKey.TICKER: "1234",
                TradeHistoryKey.NAME: "株式1",
                TradeHistoryKey.CATEGORY: Category.STOCK,
                TradeHistoryKey.PRICE: 2000,
                TradeHistoryKey.QUANTITY: 10,
            },
        ],
    )

def test_non_jpy_trade_history() -> None:
    text = "\n".join([
        '国内約定日,通貨,銘柄名,取引,預り区分,約定数量,約定単価,国内受渡日,受渡金額',
        '"2000年01月02日","日本円","KABUSHIKI A KBA / NASDAQ","買付","NISA","10","81.15","00/01/04","80000"',
        '"2000年10月10日","日本円","KABUSHIKI A KBA / NASDAQ","売却","NISA","10","83.35","00/10/13","90000"',
        '"2000年11月02日","米国ドル","KABUSHIKI E KBE / New York Stock Exchange","買付","NISA","10","43.75","00/11/04","500"',
        '"2001年02月02日","米国ドル","KABUSHIKI E KBE / New York Stock Exchange","売却","NISA","10","45.75","01/02/04","555"',
    ])

    df = read_csv(
        filepath_or_buffer=StringIO(text),
        skip_blank_lines=True,
        dtype=StringDtype(),
    )

    trade_nonjpy = parse_non_jpy_trade_history(df)

    assert_dataframe_equal(
        df = trade_nonjpy,
        expected_columns=list(TradeHistoryKey),
        expected_dtypes={
            TradeHistoryKey.DATE: "datetime64[ns]",
            TradeHistoryKey.CURRENCY: "category",
            TradeHistoryKey.ACTION: "category",
            TradeHistoryKey.EXCHANGE: "category",
            TradeHistoryKey.TICKER: "category",
            TradeHistoryKey.NAME: "category",
            TradeHistoryKey.CATEGORY: "category",
            TradeHistoryKey.PRICE: "Float64",
            TradeHistoryKey.QUANTITY: "Int64",
        },
        expected_rows=[
            {
                TradeHistoryKey.DATE: Timestamp("2000-01-02"),
                TradeHistoryKey.CURRENCY: Currency.JPY,
                TradeHistoryKey.ACTION: TradeAction.BUY,
                TradeHistoryKey.EXCHANGE: "NASDAQ",
                TradeHistoryKey.TICKER: "KBA",
                TradeHistoryKey.NAME: "KABUSHIKI A",
                TradeHistoryKey.CATEGORY: Category.STOCK,
                TradeHistoryKey.PRICE: 8000,
                TradeHistoryKey.QUANTITY: 10,
            },
            {
                TradeHistoryKey.DATE: Timestamp("2000-10-10"),
                TradeHistoryKey.CURRENCY: Currency.JPY,
                TradeHistoryKey.ACTION: TradeAction.SELL,
                TradeHistoryKey.EXCHANGE: "NASDAQ",
                TradeHistoryKey.TICKER: "KBA",
                TradeHistoryKey.NAME: "KABUSHIKI A",
                TradeHistoryKey.CATEGORY: Category.STOCK,
                TradeHistoryKey.PRICE: 9000,
                TradeHistoryKey.QUANTITY: 10,
            },
            {
                TradeHistoryKey.DATE: Timestamp("2000-11-02"),
                TradeHistoryKey.CURRENCY: Currency.USD,
                TradeHistoryKey.ACTION: TradeAction.BUY,
                TradeHistoryKey.EXCHANGE: "New York Stock Exchange",
                TradeHistoryKey.TICKER: "KBE",
                TradeHistoryKey.NAME: "KABUSHIKI E",
                TradeHistoryKey.CATEGORY: Category.STOCK,
                TradeHistoryKey.PRICE: 50,
                TradeHistoryKey.QUANTITY: 10,
            },
            {
                TradeHistoryKey.DATE: Timestamp("2001-02-02"),
                TradeHistoryKey.CURRENCY: Currency.USD,
                TradeHistoryKey.ACTION: TradeAction.SELL,
                TradeHistoryKey.EXCHANGE: "New York Stock Exchange",
                TradeHistoryKey.TICKER: "KBE",
                TradeHistoryKey.NAME: "KABUSHIKI E",
                TradeHistoryKey.CATEGORY: Category.STOCK,
                TradeHistoryKey.PRICE: 55.5,
                TradeHistoryKey.QUANTITY: 10,
            },
        ]
    )
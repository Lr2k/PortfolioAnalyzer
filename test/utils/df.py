from collections.abc import Collection

from pandas import (
    DataFrame,
    isna,
)

from pandas.api.types import (
    is_dtype_equal,
)

def _values_match(actual: object, expected: object) -> bool:
    '''
    pandas.NAを期待する場合にも対応した比較演算を行う。
    '''
    actual_isna = bool(isna(actual))
    match (isna(expected), actual_isna):
        case (True, _):
            return actual_isna
        case (False, True):
            return False
        case _:
            return actual == expected

def assert_dataframe_equal(
    df: DataFrame,
    expected_columns: Collection[str],
    expected_dtypes: dict[str, str | object],
    expected_rows: list[dict[str, object]],
):
    """
    DataFrameの列名前、dtype、値を一括で検証する。

    Parameters
    df : DataFrame
        検証対象のDataFrame
    expected_columns : Collection[str]
    expected_dtypes: dict[str, str | object]
    expected_rows: list[dict[str, object]]
    """
    # 列名検証
    assert set(df.columns) == set(expected_columns)

    # dtypeを検証
    for col_name, dtype in expected_dtypes.items():
        assert is_dtype_equal(df[col_name].dtype, dtype), \
            f"dtype mismatch. (column: {col_name}, dtype: {df[col_name].dtype} != {dtype})"
    assert len(df) == len(expected_rows)

    # 値を確認
    for row_i, expected_row in enumerate(expected_rows):
        for col_name, expected_val in expected_row.items():
            actual_val = df.loc[row_i, col_name]
            assert _values_match(actual_val, expected_val),\
                f"value mismatch. (column: {col_name}, row: {row_i}, value: {actual_val} != {expected_val})"
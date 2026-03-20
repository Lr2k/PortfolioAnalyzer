from io import StringIO

import requests as rq
from pandas import StringDtype, Float64Dtype, read_csv, to_datetime

from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.share import Category, Currency

def get_fund_code(name:str) -> tuple[str, str]:
    '''
    投信総合検索ライブラリーに投信名で検索をかけ、ISINコードと投資信託協会コードを返す。

    検索結果が0件または複数件の場合はValueErrorを送出する。

    Parameters
    ----------
    name : str
        検索する投信名。空文字・空白のみの場合はValueErrorを送出する。

    Returns
    -------
    isin_code : str
        ISINコード。
    fund_associate_code : str
        投資信託協会コード。

    Raises
    ------
    ValueError
        nameが空文字・空白のみの場合、検索結果が0件の場合、または複数件の場合。
    '''
    api_url = "https://toushin-lib.fwg.ne.jp/FdsWeb/FDST999900/fundDataSearch"
    data = {
        "t_keyword": name,
        "t_kensakuKbn": "1",
        "s_kensakuKbn": "1",
        "s_supplementKindCd": "1",
        "f_etfKBun": "1",
        "s_standardPricesCond1": "0",
        "s_standardPricesCond2": "0",
        "s_riskCond1": "0",
        "s_riskCond2": "0",
        "s_sharpCond1": "0",
        "s_sharpCond2": "0",
        "s_buyFee": "1",
        "s_trustReward": "1",
        "s_monthlyCancelCreateVal": "1",
        "s_nisaGrowthCd": "1",
        "s_investAssetKindCd": [],
        "s_investArea3kindCd": [],
        "s_instCd": [],
        "s_fdsInstCd": [],
        "s_dcFundCD": [],
        "t_investArea10kindCd": [],
        "t_investAssetKindCd": [],
        "t_instCd": [],
        "t_fdsInstCd": [],
        "s_investArea10kindCd": [],
        "s_setlFqcy": [],
        "s_dividend1y": [],
        "s_totalNetAssets": [],
        "s_nowToRedemptionDate": [],
        "s_establishedDateToNow": [],
        "s_isinCd": [],
        "startNo": 0,
        "draw": 2,
        "searchBtnClickFlg": True,
    }
    if name.strip() == '':
        raise ValueError(f"有効な投信名が指定されていません。(name:{name})")
    else:
        pass

    ses = rq.Session()
    resp_dict = ses.post(url=api_url, json=data).json()

    fund_ls = resp_dict["searchResultInfo"]["resultInfoMapList"]
    fund_infos = [(fund["fundNm"], fund["isinCd"], fund["associFundCd"]) for fund in fund_ls]
    match len(fund_infos):
        case 0:
            raise ValueError(f"投信総合検索ライブラリーから情報を取得できませんでした。(fund:{name})")
        case 1:
            return fund_infos[0][1], fund_infos[0][2]
        case n:
            raise ValueError(f"投資信託({name})について、{n}件の検索結果が得られました。\n{'\n'.join([f'{fi[0]} {fi[1]} {fi[2]}' for fi in fund_infos])}")


def get_fund_price(name: str, isin_code:str, fund_associate_code:str) -> Prices:
    '''
    投信総合検索ライブラリーから指定ファンドの基準価額履歴を取得する。

    Parameters
    ----------
    name : str
        銘柄名。PriceRecordのnameに使用される。
    isin_code : str
        ISINコード。
    fund_associate_code : str
        投資信託協会コード。

    Returns
    -------
    Prices
        全期間の基準価額履歴。
    '''
    api_url="https://toushin-lib.fwg.ne.jp/FdsWeb/FDST030000/csv-file-download"
    resp = rq.get(url=api_url+f"?isinCd={isin_code}&associFundCd={fund_associate_code}")
    resp.encoding = "shift-jis"
    csv = resp.text

    df = read_csv(StringIO(csv), dtype=StringDtype())
    dates_s = to_datetime(df["年月日"], format='%Y年%m月%d日')
    price_s = df["基準価額(円)"].astype(Float64Dtype())

    return Prices(
        records= [
            PriceRecord(
                category=Category.FUND,
                name=name,
                date=date,
                price=price,
                currency=Currency.JPY,
            )
            for date, price in zip (dates_s, price_s)
        ]
    )

if __name__=='__main__':
    import time
    name = "ＳＢＩ・ｉシェアーズ・日経２２５インデックス・ファンド"
    fund_info = get_fund_code(name)
    time.sleep(1)
    f = get_fund_price(name=name, isin_code=fund_info[0], fund_associate_code=fund_info[1])
    print(f)
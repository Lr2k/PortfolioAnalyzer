from io import StringIO

import requests as rq
from pandas import StringDtype, Float64Dtype, read_csv, to_datetime

from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.share import Currency, Category
from PortfolioAnalyzer.data.asset_id import FundID, AssetIDs

def get_fund_codes(fund_ids: AssetIDs) -> tuple[dict[str, dict[str, str]], list[FundID], dict[str, list[dict[str, str]]]]:
    '''
    投信総合検索ライブラリーに投信名で検索をかけ、ISINコードと投資信託協会コードを返す。

    Category.FUND 以外のレコードは無視される。

    Parameters
    ----------
    fund_ids : AssetIDs
        検索対象の投信識別情報。

    Returns
    -------
    name_code_map : dict[str, dict[str, str]]
        1件ヒットした投信の名前とコード情報のマッピング。
        値は ``{"isin_code": str, "fund_associate_code": str}``。
    missing_ids : list[FundID]
        検索結果が0件だった投信の識別情報リスト。
    mul_code_map : dict[str, list[dict[str, str]]]
        検索結果が複数件だった投信の候補マッピング。
        値は ``[{"name": str, "isin_code": str, "fund_associate_code": str}, ...]``。
    '''
    api_url = "https://toushin-lib.fwg.ne.jp/FdsWeb/FDST999900/fundDataSearch"
    data = {
        "t_keyword": None,
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
    ses = rq.Session()

    name_code_map: dict[str, dict[str, str]] = dict()
    missing_id_ls: list[FundID] = list()
    mul_code_map: dict[str, list[dict[str, str]]] = dict()
    for fund_id in fund_ids.records:
        if fund_id.category == Category.FUND:
            data["t_keyword"] = fund_id.name
            resp_dict = ses.post(url=api_url, json=data).json()
            fund_ls = resp_dict["searchResultInfo"]["resultInfoMapList"]
            fund_infos = [(fund["fundNm"], fund["isinCd"], fund["associFundCd"]) for fund in fund_ls]
            match len(fund_infos):
                case 0:
                    missing_id_ls.append(fund_id)
                case 1:
                    name_code_map[fund_id.name] = {
                        "isin_code": fund_infos[0][1],
                        "fund_associate_code": fund_infos[0][2],
                    }
                case n:
                    mul_code_map[fund_id.name] = [{"name":fi[0], "isin_code":fi[1], "fund_associate_code":fi[2]}  for fi in fund_infos]
        else:
            pass

    return name_code_map, AssetIDs(records=missing_id_ls), mul_code_map

def get_fund_prices(fund_ids: AssetIDs, name_code_map: dict[str, dict[str, str]]) -> tuple[Prices, AssetIDs]:
    '''
    投信総合検索ライブラリーから指定ファンドの基準価額履歴を取得する。

    Category.FUND 以外のレコードは無視される。
    name_code_map に存在しない投信は missing として返す。

    Parameters
    ----------
    fund_ids : AssetIDs
        価格を取得する投信の識別情報。
    name_code_map : dict[str, dict[str, str]]
        銘柄名からコード情報へのマッピング。``get_fund_codes`` の返り値を想定。
        値は ``{"isin_code": str, "fund_associate_code": str}``。

    Returns
    -------
    prices : Prices
        全期間の基準価額履歴（円建て）。
    missing_ids : AssetIDs
        name_code_map に存在しなかった投信の識別情報。
    '''
    api_url="https://toushin-lib.fwg.ne.jp/FdsWeb/FDST030000/csv-file-download"

    missing_id_ls: list[FundID] = list()
    price_records: list[PriceRecord] = list()
    for fund_id in fund_ids.records:
        if fund_id.category == Category.FUND:
            code_dict = name_code_map.get(fund_id.name)
            if code_dict is None:
                missing_id_ls.append(fund_id)
            else:
                resp = rq.get(url=api_url+f"?isinCd={code_dict["isin_code"]}&associFundCd={code_dict["fund_associate_code"]}")
                resp.encoding = "shift-jis"
                csv = resp.text

                df = read_csv(StringIO(csv), dtype=StringDtype())
                dates_s = to_datetime(df["年月日"], format='%Y年%m月%d日')
                price_s = df["基準価額(円)"].astype(Float64Dtype()) / 10000

                price_records += [
                    PriceRecord(
                        id=fund_id,
                        date=date,
                        price=float(price),
                        currency=Currency.JPY,
                    )
                    for date, price in zip (dates_s, price_s)
                ]
        else:
            pass

    return Prices(records=price_records), AssetIDs(records=missing_id_ls)
from typing import Optional
from pathlib import Path
from datetime import date

import typer

from PortfolioAnalyzer.gateway.sbi.csv import parse_jpy_trade_history, parse_non_jpy_trade_history, load_sbi_csv, AccountType
from PortfolioAnalyzer.gateway.gmo.api import ApiHandler
from PortfolioAnalyzer.gateway.yf.api import get_stocks_price, get_rates, RateRequest
from PortfolioAnalyzer.gateway.toushinlib.web import get_fund_codes, get_fund_prices
from PortfolioAnalyzer.data.trade import merge_trade_history, TradeHistory
from PortfolioAnalyzer.data.asset_id import merge_asset_ids
from PortfolioAnalyzer.data.asset import merge_assets
from PortfolioAnalyzer.data.price import Prices, merge_prices
from PortfolioAnalyzer.data.rate import Rates, RateRecord
from PortfolioAnalyzer.data.share import Category, Currency
from PortfolioAnalyzer.core.evaluate import evaluate as core_evaluate, convert_currency as core_convert

app = typer.Typer()

@app.callback()
def base(
    ctx: typer.Context,
    sbi_jp: list[Path] = typer.Option([], "--sbi-jp", "-j", help="SBI証券円口座の取引履歴CSV。",exists=True,dir_okay=False),
    sbi_nj: list[Path] = typer.Option([], "--sbi-nj", "-n", help="SBI証券外貨建て口座の取引履歴CSV。", exists=True, dir_okay=False),
    gmo_key: Optional[Path] = typer.Option(None, "--gmo-key", "-g", help="GMOコインのAPIキーのファイルパス。", exists=True, dir_okay=False),
):
    ctx.ensure_object(dict)
    ctx.obj["sbi_jp"] = sbi_jp
    ctx.obj["sbi_nj"] = sbi_nj
    ctx.obj["gmo_key"] = gmo_key

@app.command()
def evaluate(ctx: typer.Context):
    """
    資産の評価。
    """
    target_date = date.today()

    sbi_jp_data = [
        parse_jpy_trade_history(
            load_sbi_csv(fp, AccountType.JPY)
        ) for fp in ctx.obj["sbi_jp"]
    ]
    sbi_nj_data = [
        parse_non_jpy_trade_history(
            load_sbi_csv(fp, AccountType.NONJPY)
        
        ) for fp in ctx.obj["sbi_nj"]
    ]
    sbi_data = sbi_jp_data + sbi_nj_data
    th_ls = [th for th, _ in sbi_data]
    ids_ls = [ids for _, ids in sbi_data]

    sbi_assets = merge_trade_history(*th_ls).to_assets()
    sbi_ids = merge_asset_ids(*ids_ls)

    crypto_name_map = {"BTC": "Bitcoin", "JPY": "日本円"}
    gmo_handler = ApiHandler(ctx.obj["gmo_key"])
    gmo_assets, gmo_ids, unknown_sym = gmo_handler.get_assets(crypto_name_map)
    print(f"不明な仮想通貨シンボル: {unknown_sym}")

    total_assets = merge_assets(sbi_assets, gmo_assets)
    total_ids = merge_asset_ids(sbi_ids, gmo_ids)

    # 株式の価格
    stock_ids_ls = total_ids.get_by_category(Category.STOCK)
    exchange_map_for_yf = {"東証": "T", "--": "T", "PTS（O）":"T", "PTS（X）":"T", "NASDAQ": "", "NYSE": "", "NYSE ARCA": "", "New York Stock Exchange": ""}
    currency_map_for_yf = {"東証": Currency.JPY, "--": Currency.JPY, "PTS（O）": Currency.JPY, "PTS（X）": Currency.JPY, "NYSE ARCA": Currency.USD, "NASDAQ": Currency.USD, "NYSE": Currency.USD, "New York Stock Exchange": Currency.USD}
    stock_prices, map_missing_ids = get_stocks_price(stock_ids_ls, target_date, exchange_map_for_yf, currency_map_for_yf)
    print("必要な情報がそろっていないAssetID:")
    print(map_missing_ids)

    # 投資信託の価格
    fund_ids = total_ids.get_by_category(Category.FUND)
    name_code_map, code_missing_funds, mul_code_map = get_fund_codes(fund_ids)
    fund_prices, price_missing_funds = get_fund_prices(fund_ids, name_code_map)
    print("以下のファンドのコードが取得できませんでした。", "\n", code_missing_funds, "\n")
    print("以下のファンドのコードが複数ヒットしました。")
    for q_name, infos in mul_code_map.items():
        print(f"{q_name}:")
        for info in infos:
            print(f"  - {info["name"]}: {info["isin_code"]} {info["fund_associate_code"]}")
    print("以下のファンドの価格を取得できませんでした。")
    print(price_missing_funds)


    # 仮想通貨の価格
    crypt_ids = total_ids.get_by_category(Category.CRYPT)
    crypt_prices = gmo_handler.get_prices(crypt_ids)

    total_prices = merge_prices(stock_prices, fund_prices, crypt_prices)

    # 通貨レート
    to_currency = Currency.JPY
    from_currencies = list(total_prices.currencies)
    
    total_rates = get_rates(from_currencies=from_currencies, to_currency=to_currency, target_date=target_date)

    valuation_mul_currency, missing_assets = core_evaluate(total_assets, total_prices, target_date)
    valuation_jpy, missing_valuation = core_convert(valuation_mul_currency, total_rates, to_currency, target_date)

    print("\n\n")
    print("以下、価格情報を利用できなかった資産。")
    print(missing_assets)

    print(valuation_jpy)

    print("\n\n")
    print("以下、通貨レートを利用できなかった評価額。")
    print(missing_valuation)
    



if __name__=="__main__":
    app()
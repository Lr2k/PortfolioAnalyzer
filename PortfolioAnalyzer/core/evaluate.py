from datetime import date

from PortfolioAnalyzer.data.share import Currency
from PortfolioAnalyzer.data.asset import Assets, AssetRecord
from PortfolioAnalyzer.data.price import Prices
from PortfolioAnalyzer.data.valuation import Valuation, ValuationRecord
from PortfolioAnalyzer.data.rate import Rates


def evaluate(assets: Assets, prices: Prices, timestamp:date | None = None) -> tuple[Valuation, Assets]:
    '''
    保有資産と価格情報から評価額を計算する。

    価格情報を得られた銘柄群は評価額を計算し第1返り値として、得られず計算できなかった銘柄群を第2返り値として返す。

    Parameters
    ----------
    assets : Assets
        評価対象の保有資産。
    prices : Prices
        価格情報。複数銘柄・複数日付を含んでいてよい。
    timestamp : date | None, optional
        価格取得の基準日、および Valuation に付与する基準日。
        省略時は assets.timestamp を使用する。両方 None の場合は最新の価格を使用する。

    Returns
    -------
    valuation : Valuation
        評価額の計算結果。
    missing : Assets
        価格情報が見つからなかった銘柄の保有資産。timestamp は assets.timestamp を引き継ぐ。
    '''
    val_timestamp = timestamp or assets.timestamp

    val_records: list[ValuationRecord] = list()
    missing_assets: list[AssetRecord] = list()
    for asset in assets.records:
        price = prices.get(asset.id, val_timestamp)
        if price is None:
            missing_assets.append(asset)
        else:
            val_records.append(
                ValuationRecord(
                    id=asset.id,
                    value=asset.amount * price.price,
                    currency=price.currency,
                    date=price.date,
                )
            )

    return Valuation(records=val_records, timestamp=val_timestamp), Assets(records=missing_assets, timestamp=assets.timestamp)

def convert_currency(valuation: Valuation, rates: Rates, to_currency: Currency, timestamp: date | None = None) -> tuple[Valuation, Valuation]:
    '''
    評価額を指定通貨に換算する。

    換算できた銘柄は第1返り値、レートが見つからず換算できなかった銘柄は第2返り値として返す。
    既に to_currency の銘柄はそのまま第1返り値に含める。

    Parameters
    ----------
    valuation : Valuation
        換算対象の評価額。
    rates : Rates
        為替レート情報。
    to_currency : Currency
        換算先通貨。
    timestamp : date | None, optional
        レート取得の基準日。省略時は valuation.timestamp を使用する。
        両方 None の場合は最新のレートを使用する。

    Returns
    -------
    converted : Valuation
        換算済みの評価額。timestamp は val_timestamp。
    missing : Valuation
        レートが見つからず換算できなかった評価額。timestamp は valuation.timestamp を引き継ぐ。
    '''
    val_timestamp = timestamp or valuation.timestamp
    
    val_records: list[ValuationRecord] = list()
    missing_val_records: list[ValuationRecord] = list()
    for val in valuation.records:
        if val.currency == to_currency:
            val_records.append(val)
        else:
            rate = rates.get(from_currency=val.currency, to_currency=to_currency, target_date=val_timestamp)
            if rate is None:
                missing_val_records.append(val)
            else:
                val_records.append(
                    ValuationRecord(
                        id=val.id,
                        value=val.value * rate.rate,
                        currency=to_currency,
                        date=val.date,
                    )
                ) 
    return Valuation(records=val_records, timestamp=val_timestamp), Valuation(records=missing_val_records, timestamp=valuation.timestamp)
from datetime import date

from PortfolioAnalyzer.data.asset import Assets, AssetRecord
from PortfolioAnalyzer.data.price import Prices, PriceRecord
from PortfolioAnalyzer.data.valuation import Valuation, ValuationRecord


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
        Valuation に付与する基準日。省略時は assets.timestamp を引き継ぐ。
        両方 None の場合は None。

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
        asset_prices = {
            price.date: price
            for price in prices.records
            if (price.name == asset.name) and (price.category == asset.category)
        }
        if asset_prices:
            price:PriceRecord = asset_prices[max(asset_prices.keys())]
            val_records.append(
                ValuationRecord(
                    name=asset.name,
                    category=asset.category,
                    value=asset.amount * price.price,
                    currency=price.currency,
                    date=price.date,
                )
            )
        else:
            missing_assets.append(asset)
    
    return Valuation(records=val_records, timestamp=val_timestamp), Assets(records=missing_assets, timestamp=assets.timestamp)
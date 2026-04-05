from datetime import date

from pydantic import BaseModel, model_validator, PrivateAttr, ConfigDict

from PortfolioAnalyzer.data.share import Category, Currency


class RateRecord(BaseModel):
    '''
    単一時点における通貨ペアの為替レート。イミュータブル。

    (from_currency, to_currency) の順序に依存しない同一性を持つ。
    すなわち USD→JPY と JPY→USD は同一レコードとみなされ、
    Rates.get() で逆数として利用される。

    Attributes
    ----------
    from_currency : Currency
        変換元通貨。
    to_currency : Currency
        変換先通貨。
    date : date
        レートの基準日。
    rate : float
        from_currency 1単位あたりの to_currency の量。
    '''
    model_config = ConfigDict(frozen=True)
    from_currency: Currency
    to_currency: Currency
    date: date
    rate: float

    def __hash__(self):
        return hash((frozenset({self.from_currency, self.to_currency}), self.date))
    
    def __eq__(self, other):
        if isinstance(other, RateRecord):
            return self.date == other.date and set({self.from_currency, self.to_currency}) == set({other.from_currency, other.to_currency})
        else:
            return False
    
    def __str__(self):
        return f"{self.date} {self.to_currency}/{self.from_currency} {self.rate}"

class Rates(BaseModel):
    '''
    複数通貨ペア・複数時点の為替レートを管理するクラス。

    Attributes
    ----------
    records : list[RateRecord]
        為替レートレコードのリスト。

    Methods
    -------
    get(from_currency, to_currency, target_date)
        指定通貨ペア・日付のレートを返す。
    '''
    records: list[RateRecord] = list()
    _records_index: dict[tuple[Currency, Currency], dict[date, RateRecord]] = PrivateAttr(default_factory=dict)

    @model_validator(mode='after')
    def _build(self):
        self.records = list(set(self.records))

        for rate in self.records:
            key = (rate.from_currency, rate.to_currency)
            if key in self._records_index.keys():
                self._records_index[key][rate.date] = rate
            else:
                self._records_index[key] = {rate.date: rate}

        return self
    
    def get(self, from_currency: Currency, to_currency: Currency, target_date: date | None = None) -> RateRecord | None:
        '''
        指定通貨ペア・日付の為替レートを返す。

        逆向きのレート（to_currency → from_currency）しか登録されていない場合は
        その逆数を返す。日付が完全一致しない場合は target_date 以前で最新のレートを使用する。

        Parameters
        ----------
        from_currency : Currency
            変換元通貨。
        to_currency : Currency
            変換先通貨。
        target_date : date | None, optional
            取得対象の日付。None の場合は最新のレートを返す。

        Returns
        -------
        RateRecord | None
            見つかった場合は RateRecord、通貨ペアが存在しないまたは
            target_date 以前のデータがない場合は None。
        '''
        first_key: tuple[Currency, Currency]
        reverse_rate: bool
        _index_keys = self._records_index.keys()
        if (from_currency, to_currency) in _index_keys:
            first_key = (from_currency, to_currency)
            reverse_rate = False
        elif (to_currency, from_currency) in _index_keys:
            first_key = (to_currency, from_currency)
            reverse_rate = True
        else:
            return None

        source_date: date
        date_rate = self._records_index[first_key]
        match [dt for dt in date_rate.keys() if target_date is None or dt <= target_date]:
            case []:
                return None
            case dts:
                source_date = max(dts)

        if reverse_rate:
            return RateRecord(
                from_currency = to_currency,
                to_currency=from_currency,
                date=source_date,
                rate=1/date_rate[source_date].rate,
            )
        else:
            return date_rate[source_date]
    
    def __str__(self):
        return "\n".join([str(rate) for rate in self.records])
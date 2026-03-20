from enum import StrEnum

class Category(StrEnum):
    FUND = "投資信託"
    STOCK = "株式"
    CRYPT = "仮想通貨"
    CASH = "現金"

class Currency(StrEnum):
    JPY = "JPY"
    USD = "USD"

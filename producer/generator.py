# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import random
import string
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional, Tuple


MERCHANT_CATALOG: List[Tuple[str, str]] = [
    ("ACME Coffee Shop", "5814"),
    ("Bahn Tickets Online", "4112"),
    ("REWE Supermarket", "5411"),
    ("Lufthansa", "4511"),
    ("Shell Tankstelle", "5541"),
    ("Apple Store", "5732"),
    ("Amazon.de", "5942"),
    ("Netflix", "4899"),
    ("IKEA", "5712"),
    ("Restaurant Zum Adler", "5812"),
    ("Bayer Apotheke", "5912"),
    ("Hotel Adlon", "7011"),
    ("MediaMarkt", "5732"),
    ("DHL Paketshop", "4215"),
    ("ATM Withdrawal", "6011"),
    ("Steam Games", "5816"),
    ("Booking.com", "4722"),
    ("Uber", "4121"),
    ("Spotify", "5815"),
    ("DM Drogerie", "5912"),
]

COUNTRY_GEO: Dict[str, Tuple[str, float, float]] = {
    "DE": ("Berlin", 52.520, 13.405),
    "FR": ("Paris", 48.857, 2.352),
    "GB": ("London", 51.507, -0.128),
    "US": ("New York", 40.713, -74.006),
    "ES": ("Madrid", 40.417, -3.704),
    "IT": ("Rome", 41.903, 12.496),
    "NL": ("Amsterdam", 52.370, 4.895),
    "BR": ("Sao Paulo", -23.551, -46.634),
    "NG": ("Lagos", 6.524, 3.379),
    "RU": ("Moscow", 55.756, 37.617),
    "CN": ("Shanghai", 31.230, 121.474),
}

HOME_COUNTRIES = ["DE", "FR", "GB", "ES", "IT", "NL"]
FOREIGN_HIGH_RISK = ["NG", "RU", "CN", "BR"]

CHANNELS = ["chip", "contactless", "magstripe", "online", "atm"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.05, 0.25, 0.05]

FRAUD_PATTERNS = ("velocity", "geo_jump", "high_amount", "card_testing", "cnp_foreign")


@dataclass
class Account:
    account_id: str
    card_id: str
    card_bin: str
    home_country: str
    typical_amount: float
    currency: str


@dataclass
class Transaction:
    transaction_id: str
    event_time: str
    account_id: str
    card_id: str
    card_bin: str
    merchant_id: str
    merchant_name: str
    merchant_mcc: str
    merchant_country: str
    merchant_city: str
    amount: float
    currency: str
    channel: str
    device_id: Optional[str]
    ip_address: Optional[str]
    lat: float
    lon: float
    is_fraud: bool
    fraud_pattern: Optional[str]

    def as_dict(self) -> Dict:
        return self.__dict__.copy()


def _luhn_check_digit(number_without_check: str) -> str:
    digits = [int(d) for d in number_without_check]
    odd_sum = sum(digits[-1::-2])
    even_sum = sum(sum(divmod(d * 2, 10)) for d in digits[-2::-2])
    return str((10 - (odd_sum + even_sum) % 10) % 10)


def _new_card_number(bin_prefix: str, rng: random.Random) -> str:
    body = bin_prefix + "".join(rng.choices(string.digits, k=15 - len(bin_prefix)))
    return body + _luhn_check_digit(body)


def _new_id(prefix: str, rng: random.Random, width: int = 12) -> str:
    return f"{prefix}_{''.join(rng.choices(string.ascii_lowercase + string.digits, k=width))}"


def _new_ip(rng: random.Random) -> str:
    return ".".join(str(rng.randint(1, 254)) for _ in range(4))


class TransactionGenerator:
    """Generates a synthetic stream of card transactions for fraud-detection testing.

    The generator maintains a fixed pool of accounts/merchants so messages are
    correlated (the same card appears repeatedly), which is what fraud models
    expect. A configurable fraction of emissions are fraud bursts that follow
    one of several documented patterns.
    """

    def __init__(
        self,
        num_accounts: int,
        num_merchants: int,
        fraud_rate: float,
        seed: Optional[int] = None,
    ):
        self.rng = random.Random(seed)
        self.fraud_rate = fraud_rate
        self.accounts = self._make_accounts(num_accounts)
        self.merchants = self._make_merchants(num_merchants)
        self._pending: Deque[Transaction] = deque()

    def _make_accounts(self, n: int) -> List[Account]:
        bins = ["549920", "411111", "521234", "374200"]
        accounts: List[Account] = []
        for i in range(n):
            bin_prefix = self.rng.choice(bins)
            country = self.rng.choice(HOME_COUNTRIES)
            accounts.append(
                Account(
                    account_id=f"acct_{i:08d}",
                    card_id=_new_card_number(bin_prefix, self.rng),
                    card_bin=bin_prefix,
                    home_country=country,
                    typical_amount=round(self.rng.uniform(10, 120), 2),
                    currency="EUR" if country != "US" and country != "GB" else ("USD" if country == "US" else "GBP"),
                )
            )
        return accounts

    def _make_merchants(self, n: int) -> List[Dict]:
        merchants: List[Dict] = []
        for i in range(n):
            name, mcc = self.rng.choice(MERCHANT_CATALOG)
            country = self.rng.choice(HOME_COUNTRIES)
            city, lat, lon = COUNTRY_GEO[country]
            merchants.append(
                {
                    "merchant_id": f"mer_{i:06d}",
                    "merchant_name": name,
                    "merchant_mcc": mcc,
                    "merchant_country": country,
                    "merchant_city": city,
                    "lat": lat + self.rng.uniform(-0.1, 0.1),
                    "lon": lon + self.rng.uniform(-0.1, 0.1),
                }
            )
        return merchants

    def next(self) -> Transaction:
        if self._pending:
            return self._pending.popleft()

        if self.rng.random() < self.fraud_rate:
            burst = self._generate_fraud_burst()
            self._pending.extend(burst[1:])
            return burst[0]

        return self._legit_transaction()

    def _legit_transaction(self, now: Optional[datetime] = None) -> Transaction:
        account = self.rng.choice(self.accounts)
        merchant = self.rng.choice(self.merchants)
        return self._build(account, merchant, now=now, is_fraud=False, fraud_pattern=None)

    def _build(
        self,
        account: Account,
        merchant: Dict,
        now: Optional[datetime] = None,
        is_fraud: bool = False,
        fraud_pattern: Optional[str] = None,
        amount: Optional[float] = None,
        channel: Optional[str] = None,
        country_override: Optional[str] = None,
    ) -> Transaction:
        now = now or datetime.now(timezone.utc)
        if amount is None:
            amount = round(max(0.5, self.rng.gauss(account.typical_amount, account.typical_amount * 0.4)), 2)
        if channel is None:
            channel = self.rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        if country_override:
            city, lat, lon = COUNTRY_GEO[country_override]
            merchant = {**merchant, "merchant_country": country_override, "merchant_city": city, "lat": lat, "lon": lon}
        return Transaction(
            transaction_id=_new_id("txn", self.rng, 16),
            event_time=now.isoformat().replace("+00:00", "Z"),
            account_id=account.account_id,
            card_id=account.card_id,
            card_bin=account.card_bin,
            merchant_id=merchant["merchant_id"],
            merchant_name=merchant["merchant_name"],
            merchant_mcc=merchant["merchant_mcc"],
            merchant_country=merchant["merchant_country"],
            merchant_city=merchant["merchant_city"],
            amount=amount,
            currency=account.currency,
            channel=channel,
            device_id=_new_id("dev", self.rng, 10) if channel == "online" else None,
            ip_address=_new_ip(self.rng) if channel == "online" else None,
            lat=merchant["lat"],
            lon=merchant["lon"],
            is_fraud=is_fraud,
            fraud_pattern=fraud_pattern,
        )

    def _generate_fraud_burst(self) -> List[Transaction]:
        pattern = self.rng.choice(FRAUD_PATTERNS)
        account = self.rng.choice(self.accounts)
        now = datetime.now(timezone.utc)

        if pattern == "velocity":
            merchant = self.rng.choice(self.merchants)
            return [
                self._build(
                    account, merchant,
                    now=now + timedelta(seconds=i * 3),
                    amount=round(self.rng.uniform(0.5, 5.0), 2),
                    channel="online",
                    is_fraud=True, fraud_pattern=pattern,
                )
                for i in range(self.rng.randint(6, 12))
            ]

        if pattern == "geo_jump":
            home_merchant = next(m for m in self.merchants if m["merchant_country"] == account.home_country)
            foreign_country = self.rng.choice(FOREIGN_HIGH_RISK)
            foreign_merchant = self.rng.choice(self.merchants)
            return [
                self._build(account, home_merchant, now=now, is_fraud=True, fraud_pattern=pattern),
                self._build(
                    account, foreign_merchant,
                    now=now + timedelta(minutes=self.rng.randint(2, 8)),
                    amount=round(self.rng.uniform(80, 600), 2),
                    country_override=foreign_country,
                    is_fraud=True, fraud_pattern=pattern,
                ),
            ]

        if pattern == "high_amount":
            merchant = self.rng.choice(self.merchants)
            anomaly = account.typical_amount * self.rng.uniform(25, 80)
            return [self._build(account, merchant, now=now, amount=round(anomaly, 2),
                                is_fraud=True, fraud_pattern=pattern)]

        if pattern == "card_testing":
            return [
                self._build(
                    account, self.rng.choice(self.merchants),
                    now=now + timedelta(seconds=i * 7),
                    amount=round(self.rng.uniform(0.5, 2.5), 2),
                    channel="online",
                    is_fraud=True, fraud_pattern=pattern,
                )
                for i in range(self.rng.randint(4, 9))
            ]

        # cnp_foreign
        foreign_country = self.rng.choice(FOREIGN_HIGH_RISK)
        merchant = self.rng.choice(self.merchants)
        return [self._build(
            account, merchant, now=now,
            channel="online",
            country_override=foreign_country,
            amount=round(self.rng.uniform(150, 1200), 2),
            is_fraud=True, fraud_pattern=pattern,
        )]

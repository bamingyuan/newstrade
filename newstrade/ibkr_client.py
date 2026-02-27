from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .market_data import pct_change

try:
    from ib_insync import IB, Stock, ScannerSubscription
except ImportError:  # pragma: no cover - dependency validated at runtime
    IB = None
    Stock = None
    ScannerSubscription = None


@dataclass
class IbkrConnectionConfig:
    host: str
    port: int
    client_id: int


class IbkrClient:
    def __init__(self, host: str, port: int, client_id: int) -> None:
        self._cfg = IbkrConnectionConfig(host=host, port=port, client_id=client_id)
        self.ib: Any = None

    def connect(self) -> None:
        if IB is None:
            raise RuntimeError("ib_insync is not installed. Please install dependencies first.")
        if self.ib is None:
            self.ib = IB()
        if not self.ib.isConnected():
            self.ib.connect(self._cfg.host, self._cfg.port, clientId=self._cfg.client_id, timeout=8)

    def disconnect(self) -> None:
        if self.ib is not None and self.ib.isConnected():
            self.ib.disconnect()

    def __enter__(self) -> "IbkrClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    def discover_symbols(self, max_symbols: int = 100) -> list[str]:
        self.connect()
        symbols: set[str] = set()

        scan_codes = ["TOP_PERC_GAIN", "TOP_PERC_LOSE"]
        for scan_code in scan_codes:
            sub = ScannerSubscription(
                instrument="STK",
                locationCode="STK.US.MAJOR",
                scanCode=scan_code,
            )
            try:
                data = self.ib.reqScannerData(sub)
            except Exception:
                continue
            for row in data[:max_symbols]:
                contract = row.contractDetails.contract
                if contract.symbol:
                    symbols.add(contract.symbol.upper())

        return sorted(symbols)

    def fetch_price_snapshot(
        self,
        symbol: str,
        intraday_lookback_days: int,
        intraday_bar_size: str,
    ) -> dict[str, Any]:
        self.connect()
        contract = Stock(symbol, "SMART", "USD")
        self.ib.qualifyContracts(contract)

        daily_bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="10 D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )

        intraday_bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=f"{intraday_lookback_days} D",
            barSizeSetting=intraday_bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )

        last_price = None
        pct_change_1d = None
        pct_change_intraday = None

        if daily_bars:
            closes = [float(bar.close) for bar in daily_bars if getattr(bar, "close", None) is not None]
            if closes:
                last_price = closes[-1]
            if len(closes) >= 2:
                pct_change_1d = pct_change(closes[-2], closes[-1])

        if intraday_bars:
            closes = [float(bar.close) for bar in intraday_bars if getattr(bar, "close", None) is not None]
            if closes and last_price is None:
                last_price = closes[-1]
            if len(closes) >= 2:
                pct_change_intraday = pct_change(closes[0], closes[-1])

        if last_price is None:
            raise RuntimeError(f"No price bars returned for {symbol}")

        return {
            "symbol": symbol,
            "last_price": float(last_price),
            "pct_change_1d": pct_change_1d,
            "pct_change_intraday": pct_change_intraday,
            "price_source_ts_utc": datetime.now(timezone.utc).isoformat(),
        }


def create_ibkr_client(host: str, port: int, client_id: int) -> IbkrClient:
    return IbkrClient(host=host, port=port, client_id=client_id)

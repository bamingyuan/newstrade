from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import logging
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


logger = logging.getLogger(__name__)


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
            logger.debug(
                "IBKR connect request host=%s port=%s clientId=%s timeout=%s",
                self._cfg.host,
                self._cfg.port,
                self._cfg.client_id,
                8,
            )
            self.ib.connect(self._cfg.host, self._cfg.port, clientId=self._cfg.client_id, timeout=8)
            logger.debug("IBKR connect response connected=%s", self.ib.isConnected())

    def disconnect(self) -> None:
        if self.ib is not None and self.ib.isConnected():
            logger.debug("IBKR disconnect request")
            self.ib.disconnect()
            logger.debug("IBKR disconnect response connected=%s", self.ib.isConnected())

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
                logger.debug(
                    "IBKR reqScannerData request instrument=%s locationCode=%s scanCode=%s max_symbols=%s",
                    sub.instrument,
                    sub.locationCode,
                    sub.scanCode,
                    max_symbols,
                )
                data = self.ib.reqScannerData(sub)
                logger.debug(
                    "IBKR reqScannerData response scanCode=%s rows=%s",
                    scan_code,
                    len(data),
                )
            except Exception:
                logger.exception("IBKR reqScannerData failed for scanCode=%s", scan_code)
                continue
            for row in data[:max_symbols]:
                contract = row.contractDetails.contract
                if contract.symbol:
                    symbols.add(contract.symbol.upper())

        discovered = sorted(symbols)
        logger.debug(
            "IBKR discover_symbols result count=%s symbols=%s",
            len(discovered),
            discovered,
        )
        return discovered

    def fetch_price_snapshot(
        self,
        symbol: str,
        intraday_lookback_days: int,
        intraday_bar_size: str,
        end_datetime: datetime | None = None,
    ) -> dict[str, Any]:
        self.connect()
        contract = Stock(symbol, "SMART", "USD")
        logger.debug(
            "IBKR qualifyContracts request symbol=%s exchange=%s currency=%s",
            symbol,
            "SMART",
            "USD",
        )
        self.ib.qualifyContracts(contract)
        logger.debug(
            "IBKR qualifyContracts response symbol=%s conId=%s primaryExchange=%s",
            symbol,
            getattr(contract, "conId", None),
            getattr(contract, "primaryExchange", None),
        )
        end_date_time = _format_ib_end_datetime(end_datetime)

        logger.debug(
            "IBKR reqHistoricalData request symbol=%s endDateTime=%r durationStr=%s barSizeSetting=%s whatToShow=%s useRTH=%s formatDate=%s",
            symbol,
            end_date_time,
            "10 D",
            "1 day",
            "TRADES",
            True,
            1,
        )
        daily_bars = self.ib.reqHistoricalData(
            contract,
            endDateTime=end_date_time,
            durationStr="10 D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        logger.debug(
            "IBKR reqHistoricalData response symbol=%s durationStr=%s barSizeSetting=%s bars=%s",
            symbol,
            "10 D",
            "1 day",
            len(daily_bars),
        )

        logger.debug(
            "IBKR reqHistoricalData request symbol=%s endDateTime=%r durationStr=%s barSizeSetting=%s whatToShow=%s useRTH=%s formatDate=%s",
            symbol,
            end_date_time,
            f"{intraday_lookback_days} D",
            intraday_bar_size,
            "TRADES",
            True,
            1,
        )
        intraday_bars = self.ib.reqHistoricalData(
            contract,
            endDateTime=end_date_time,
            durationStr=f"{intraday_lookback_days} D",
            barSizeSetting=intraday_bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        logger.debug(
            "IBKR reqHistoricalData response symbol=%s durationStr=%s barSizeSetting=%s bars=%s",
            symbol,
            f"{intraday_lookback_days} D",
            intraday_bar_size,
            len(intraday_bars),
        )

        last_price = None
        pct_change_1d = None
        pct_change_intraday = None
        latest_daily_bar_date: date | None = None
        latest_daily_volume: float | None = None

        if daily_bars:
            closes = [float(bar.close) for bar in daily_bars if getattr(bar, "close", None) is not None]
            if closes:
                last_price = closes[-1]
            latest_daily_bar_date = _parse_bar_date(getattr(daily_bars[-1], "date", None))
            latest_daily_volume = _as_float(getattr(daily_bars[-1], "volume", None))
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

        snapshot = {
            "symbol": symbol,
            "last_price": float(last_price),
            "pct_change_1d": pct_change_1d,
            "pct_change_intraday": pct_change_intraday,
            "volume": latest_daily_volume,
            "latest_daily_bar_date": latest_daily_bar_date.isoformat() if latest_daily_bar_date else None,
            "price_source_ts_utc": datetime.now(timezone.utc).isoformat(),
            "price_as_of_ts_utc": (
                end_datetime.astimezone(timezone.utc).isoformat()
                if end_datetime is not None
                else datetime.now(timezone.utc).isoformat()
            ),
        }
        logger.debug(
            "IBKR snapshot result symbol=%s last_price=%s pct_change_1d=%s pct_change_intraday=%s volume=%s latest_daily_bar_date=%s",
            symbol,
            snapshot["last_price"],
            snapshot["pct_change_1d"],
            snapshot["pct_change_intraday"],
            snapshot["volume"],
            snapshot["latest_daily_bar_date"],
        )
        return snapshot


def create_ibkr_client(host: str, port: int, client_id: int) -> IbkrClient:
    return IbkrClient(host=host, port=port, client_id=client_id)


def _format_ib_end_datetime(end_datetime: datetime | None) -> str:
    if end_datetime is None:
        return ""
    return end_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")


def _parse_bar_date(raw: Any) -> date | None:
    if isinstance(raw, datetime):
        return raw.date()

    text = str(raw or "").strip()
    if not text:
        return None

    digits = "".join(char for char in text if char.isdigit())
    if len(digits) >= 8:
        try:
            return datetime.strptime(digits[:8], "%Y%m%d").date()
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _as_float(raw: Any) -> float | None:
    try:
        if raw is None:
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None

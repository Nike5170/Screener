import aiohttp
import asyncio
import math
from config import (
    BINANCE_24HR_URL,
    BINANCE_DEPTH_URL,
    BINANCE_INFO_URL,
    EXCLUDE_SYMBOLS,
    HTTP_CONCURRENCY,
    HTTP_TIMEOUT_SEC,
    MIN_TRADES,
    ORDERBOOK_DEPTH_PERCENT,
    ORDERBOOK_MIN_ASK_VOLUME,
    ORDERBOOK_MIN_BID_VOLUME,
    VOLUME_THRESHOLD,
    IMPULSE_VOL_MIN,
    IMPULSE_VOL_MAX,
    IMPULSE_P_MIN,
    IMPULSE_P_MAX,
    IMPULSE_EXPONENT,
    ENABLE_DYNAMIC_THRESHOLD,
    IMPULSE_FIXED_THRESHOLD_PCT,
    ORDERBOOK_REQUEST_DELAY,
    BINANCE_SPOT_INFO_URL,
    EXTERNAL_MARKETS_ENABLED,
    EXTERNAL_MARKETS_MAX_PER_SYMBOL,
    FX_KRW_USD_URL,
    MARKET_TICKER_ENDPOINTS,
    TIGER_TICKER_FORMATS,
)

from logger import Logger

http_semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)

def to_int_round(x, default: int = 0) -> int:
    try:
        return int(round(float(x)))
    except Exception:
        return int(default)


def dynamic_impulse_threshold(volume):
    """
    Вычисление индивидуального порога импульса для символа на основе 24h объёма.
    """
    v_min = IMPULSE_VOL_MIN
    v_max = IMPULSE_VOL_MAX
    p_min = IMPULSE_P_MIN
    p_max = IMPULSE_P_MAX

    x = min(max(volume, v_min), v_max)
    norm = (math.log10(x) - math.log10(v_min)) / (math.log10(v_max) - math.log10(v_min))
    factor = norm ** IMPULSE_EXPONENT
    percent = p_max - (p_max - p_min) * factor
    return round(percent, 3)

def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)

def _tiger_symbol(exchange: str, market: str, base: str, quote: str | None) -> str:
    fmt = (TIGER_TICKER_FORMATS.get(exchange, {}) or {}).get(market)
    if not fmt:
        return ""
    if "{quote}" in fmt and not quote:
        return ""
    return fmt.format(base=base, quote=(quote or ""))

def _base_from_binance_usdt_perp(sym_up: str) -> str:
    if sym_up.endswith("USDT") and len(sym_up) > 4:
        return sym_up[:-4]
    return sym_up

class SymbolFetcher:
    async def fetch_futures_symbols(self):
        url_info = BINANCE_INFO_URL
        url_24hr = BINANCE_24HR_URL
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:

                # ----------------------------------------------------
                # 1) exchangeInfo
                # ----------------------------------------------------
                try:
                    async with session.get(url_info) as info:
                        info_data = await info.json()
                except Exception as e:
                    Logger.error(f"❌ Ошибка exchangeInfo: {e}")
                    return {"volumes": {}, "thresholds": {}}

                if "symbols" not in info_data:
                    Logger.error(f"❌ Некорректный ответ exchangeInfo: {info_data}")
                    return {"volumes": {}, "thresholds": {}}

                active_symbols = {
                    s["symbol"]
                    for s in info_data["symbols"]
                    if s.get("status") == "TRADING"
                    and s.get("quoteAsset") == "USDT"
                    and s.get("contractType", "").endswith("PERPETUAL")
                    and s["symbol"] not in EXCLUDE_SYMBOLS
                }

                Logger.success(f"Всего символов с USDT и PERPETUAL: {len(active_symbols)}")

                # ----------------------------------------------------
                # 2) 24hr
                # ----------------------------------------------------
                try:
                    async with session.get(url_24hr) as r:
                        data = await r.json()
                except Exception as e:
                    Logger.error(f"❌ Ошибка получения 24hr данных: {e}")
                    return {"volumes": {}, "thresholds": {}}

                if not isinstance(data, list):
                    Logger.error(f"❌ Некорректный ответ 24hr: {data}")
                    return {"volumes": {}, "thresholds": {}}

                # ----------------------------------------------------
                # 3) Фильтр по объёму
                # ----------------------------------------------------
                try:
                    filtered_by_volume = [
                        d for d in data
                        if d["symbol"] in active_symbols
                        and to_int_round(d.get("quoteVolume", 0)) >= int(VOLUME_THRESHOLD)
                    ]
                except Exception as e:
                    Logger.error(f"❌ Ошибка фильтра по объёму: {e}")
                    return {"volumes": {}, "thresholds": {}}

                Logger.info(f"После фильтра по объёму ≥{VOLUME_THRESHOLD}: {len(filtered_by_volume)}")

                # ----------------------------------------------------
                # 4) Фильтр по числу сделок
                # ----------------------------------------------------
                try:
                    filtered_by_trades = [
                        d for d in filtered_by_volume
                        if int(d.get("count", 0)) >= MIN_TRADES
                    ]
                except Exception as e:
                    Logger.error(f"❌ Ошибка фильтра по сделкам: {e}")
                    return {"volumes": {}, "thresholds": {}}

                Logger.info(f"После фильтра по сделкам ≥{MIN_TRADES}: {len(filtered_by_trades)}")

                # ----------------------------------------------------
                # 5) Фильтр по стакану
                # ----------------------------------------------------
                sorted_by_volume = sorted(
                    filtered_by_trades,
                    key=lambda x: to_int_round(x.get("quoteVolume", 0)),
                    reverse=True
                )

                filtered_depth = []
                for i, d in enumerate(sorted_by_volume):
                    symbol = d["symbol"].lower()

                    ok = False
                    try:
                        ok, bid_v, ask_v = await self.check_order_book_volume(session, symbol)
                        await asyncio.sleep(ORDERBOOK_REQUEST_DELAY)
                        d["_bid_vol"] = bid_v
                        d["_ask_vol"] = ask_v
                    except Exception as e:
                        Logger.error(f"❌ Ошибка глубины стакана {symbol}: {e}")
                        ok = False

                    if ok:
                        filtered_depth.append(d)
                    else:
                        Logger.warn(f"❌ {symbol.upper()} отфильтрован по глубине стакана")

                    if i % 10 == 0 and i > 0:
                        await asyncio.sleep(1)

                Logger.success(f"После фильтра по стакану осталось: {len(filtered_depth)}")

                # ----------------------------------------------------
                # 6) Финальная сортировка
                # ----------------------------------------------------
                sorted_final = sorted(
                    filtered_depth,
                    key=lambda x: to_int_round(x.get("quoteVolume", 0)),
                    reverse=True
                )


                # ----------------------------------------------------
                # 7) Индивидуальные thresholds
                # ----------------------------------------------------
                symbol_thresholds = {}
                volumes = {}
                trades24h = {}
                orderbook = {}

                for d in sorted_final:
                    symbol = d["symbol"].lower()
                    volume_i = to_int_round(d.get("quoteVolume", 0))
                    volumes[symbol] = volume_i

                    if ENABLE_DYNAMIC_THRESHOLD:
                        symbol_thresholds[symbol] = dynamic_impulse_threshold(volume_i)
                    else:
                        symbol_thresholds[symbol] = float(IMPULSE_FIXED_THRESHOLD_PCT)

                    trades24h[symbol] = int(d.get("count", 0))

                    orderbook[symbol] = {
                        "bid": int(d.get("_bid_vol", 0) or 0),
                        "ask": int(d.get("_ask_vol", 0) or 0),
                    }


                Logger.info(f"Всего символов после фильтров: {len(volumes)}")

                return {
                "volumes": volumes,
                "thresholds": symbol_thresholds,
                "trades24h": trades24h,
                "orderbook": orderbook
                }


        except asyncio.TimeoutError:
            Logger.error("⏳ Глобальный timeout fetch_futures_symbols()")
            return {"volumes": {}, "thresholds": {}}

        except aiohttp.ClientError as e:
            Logger.error(f"🌐 Ошибка сети Binance: {e}")
            return {"volumes": {}, "thresholds": {}}

        except asyncio.CancelledError:
            Logger.error("❌ fetch_futures_symbols() был отменён (CancelledError)")
            return {"volumes": {}, "thresholds": {}}

        except Exception as e:
            Logger.error(f"🔥 Неизвестная ошибка: {e}")
            return {"volumes": {}, "thresholds": {}}


    # ============================================================
    #                Проверка стакана
    # ============================================================
    async def fetch_spot_symbols(self) -> set[str]:
        """
        Возвращает set(symbol.lower()) для активных Binance SPOT USDT пар
        """
        symbols = set()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_SPOT_INFO_URL, timeout=10) as resp:
                    data = await resp.json()

            for s in data.get("symbols", []):
                if s.get("status") != "TRADING":
                    continue

                if s.get("quoteAsset") != "USDT":
                    continue

                # если есть permissions — проверяем SPOT
                perms = s.get("permissions")
                if perms and "SPOT" not in perms:
                    continue

                symbols.add(s["symbol"].lower())

        except Exception as e:
            Logger.error(f"fetch_spot_symbols error: {e}")

        return symbols
    
    async def check_order_book_volume(self, session, symbol):
        url_depth = f"{BINANCE_DEPTH_URL}?symbol={symbol.upper()}&limit=500"
        async with http_semaphore:
            try:
                async with session.get(url_depth) as resp:
                    data = await resp.json()
            except Exception as e:
                Logger.error(f"❌ Ошибка получения стакана для {symbol}: {e}")
                return False, 0.0, 0.0

            if "bids" not in data or "asks" not in data:
                Logger.error(f"❌ Некорректный стакан {symbol.upper()}: {data}")
                return False, 0.0, 0.0

            if not data["bids"] or not data["asks"]:
                Logger.error(f"❌ Пустой стакан {symbol.upper()}")
                return False, 0.0, 0.0

            try:
                bids = [(float(p), float(q)) for p, q in data["bids"]]
                asks = [(float(p), float(q)) for p, q in data["asks"]]
            except Exception as e:
                Logger.error(f"❌ Ошибка парсинга стакана {symbol.upper()}: {e}")
                return False, 0.0, 0.0

            price = (bids[0][0] + asks[0][0]) / 2
            lower_bound = price * (1 - ORDERBOOK_DEPTH_PERCENT)
            upper_bound = price * (1 + ORDERBOOK_DEPTH_PERCENT)

            bid_volume = sum(p * q for p, q in bids if p >= lower_bound)
            ask_volume = sum(p * q for p, q in asks if p <= upper_bound)

            bid_volume_i = to_int_round(bid_volume)
            ask_volume_i = to_int_round(ask_volume)

            ok = bid_volume_i >= int(ORDERBOOK_MIN_BID_VOLUME) and ask_volume_i >= int(ORDERBOOK_MIN_ASK_VOLUME)
            return ok, bid_volume_i, ask_volume_i

    async def fetch_external_markets(self, binance_usdt_symbols: list[str]) -> dict[str, list[dict]]:
        """
        Returns:
        { "BTCUSDT": [ {exchange, market, tiger, volume_usd, quote}, ... ], ... }
        Sorted by volume_usd desc.
        """
        if not EXTERNAL_MARKETS_ENABLED:
            return {}

        bases: set[str] = set()
        for s in binance_usdt_symbols:
            bases.add(_base_from_binance_usdt_perp(str(s).upper()))

        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
        out: dict[str, list[dict]] = {}

        krw_usd = 0.0
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(FX_KRW_USD_URL) as r:
                    j = await r.json()
                    krw_usd = _safe_float(j.get("result"), 0.0)
        except Exception:
            krw_usd = 0.0

        bybit_spot, bybit_fut = {}, {}
        okx_spot, okx_swap = {}, {}
        mexc_spot = {}
        gate_spot, gate_fut = {}, {}
        bitget_spot, bitget_fut = {}, {}
        aster_spot, aster_fut = {}, {}
        upbit = {}
        hyper = {}
        backpack_spot, backpack_fut = {}, {}

        async with aiohttp.ClientSession(timeout=timeout) as session:
            # BYBIT
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["BYBIT_SPOT"]) as r:
                    j = await r.json()
                    lst = (((j or {}).get("result") or {}).get("list") or [])
                    for it in lst:
                        sym = str(it.get("symbol") or "").upper()
                        vol = _safe_float(it.get("turnover24h"), 0.0)
                        if sym:
                            bybit_spot[sym] = vol
            except Exception:
                pass

            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["BYBIT_FUT"]) as r:
                    j = await r.json()
                    lst = (((j or {}).get("result") or {}).get("list") or [])
                    for it in lst:
                        sym = str(it.get("symbol") or "").upper()
                        vol = _safe_float(it.get("turnover24h"), 0.0)
                        if sym:
                            bybit_fut[sym] = vol
            except Exception:
                pass

            # OKX
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["OKX_SPOT"]) as r:
                    j = await r.json()
                    lst = (j or {}).get("data") or []
                    for it in lst:
                        inst = str(it.get("instId") or "").upper()
                        # volCcy24h = volume in quote currency (USDT)
                        vol = _safe_float(it.get("volCcy24h") or it.get("vol24h") or 0.0, 0.0)
                        if inst:
                            okx_spot[inst] = vol
            except Exception as e:
                Logger.error(f"OKX SPOT load error: {e}")

            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["OKX_SWAP"]) as r:
                    j = await r.json()
                    lst = (j or {}).get("data") or []
                    for it in lst:
                        inst = str(it.get("instId") or "").upper()
                        # volCcy24h = volume in quote currency (USDT)
                        vol = _safe_float(it.get("volCcy24h") or it.get("vol24h") or 0.0, 0.0)
                        if inst:
                            okx_swap[inst] = vol
            except Exception as e:
                Logger.error(f"OKX SWAP load error: {e}")

            # MEXC spot
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["MEXC_SPOT"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("symbol") or "").upper()
                            vol = _safe_float(it.get("quoteVolume"), 0.0)
                            if sym:
                                mexc_spot[sym] = vol
            except Exception:
                pass

            # Gate.io
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["GATE_SPOT"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("currency_pair") or "").upper()
                            vol = _safe_float(it.get("quote_volume"), 0.0)
                            if sym:
                                gate_spot[sym] = vol
            except Exception:
                pass

            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["GATE_FUT"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("contract") or "").upper()
                            vol = _safe_float(
                                it.get("volume_24h_quote")
                                or it.get("volume_24h")
                                or it.get("quote_volume")
                                or 0.0,
                                0.0,
                            )
                            if sym:
                                gate_fut[sym] = vol
            except Exception:
                pass

            # Bitget
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["BITGET_SPOT"]) as r:
                    j = await r.json()
                    lst = ((j or {}).get("data") or [])
                    for it in lst:
                        sym = str(it.get("symbol") or "").upper()
                        vol = _safe_float(it.get("quoteVolume") or it.get("usdtVolume") or 0.0, 0.0)
                        if sym:
                            bitget_spot[sym] = vol
            except Exception:
                pass

            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["BITGET_FUT"]) as r:
                    j = await r.json()
                    lst = ((j or {}).get("data") or [])
                    for it in lst:
                        sym = str(it.get("symbol") or "").upper()
                        vol = _safe_float(it.get("quoteVolume") or it.get("usdtVolume") or it.get("turnover") or 0.0, 0.0)
                        if sym:
                            bitget_fut[sym] = vol
            except Exception:
                pass

            # AsterDex
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["ASTER_SPOT_24HR"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("symbol") or "").upper()
                            vol = _safe_float(it.get("quoteVolume"), 0.0)
                            if sym:
                                aster_spot[sym] = vol
            except Exception:
                pass

            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["ASTER_FUT_24HR"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("symbol") or "").upper()
                            vol = _safe_float(it.get("quoteVolume"), 0.0)
                            if sym:
                                aster_fut[sym] = vol
            except Exception:
                pass

            # Upbit KRW
            try:
                markets = [f"KRW-{b}" for b in bases]
                markets_csv = ",".join(markets)
                url = MARKET_TICKER_ENDPOINTS["UPBIT_TICKER"].format(markets_csv=markets_csv)
                async with session.get(url) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            m = str(it.get("market") or "").upper()
                            vol_krw = _safe_float(it.get("acc_trade_price_24h"), 0.0)
                            vol_usd = vol_krw * krw_usd if krw_usd > 0 else 0.0
                            if m:
                                upbit[m] = vol_usd
            except Exception as e:
                Logger.error(f"Upbit load error: {e}")

            # Hyperliquid
            try:
                body = {"type": "metaAndAssetCtxs"}
                async with session.post(MARKET_TICKER_ENDPOINTS["HYPERLIQUID_INFO"], json=body) as r:
                    j = await r.json()
                    if isinstance(j, list) and len(j) >= 2:
                        meta = j[0] or {}
                        ctxs = j[1] or []
                        universe = (meta.get("universe") or [])
                        for i, u in enumerate(universe):
                            name = str((u or {}).get("name") or "").upper()
                            if i < len(ctxs):
                                day = _safe_float((ctxs[i] or {}).get("dayNtlVlm"), 0.0)
                                if name:
                                    hyper[name] = day
            except Exception as e:
                Logger.error(f"Hyperliquid load error: {e}")

            # Backpack
            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["BACKPACK_SPOT"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("symbol") or "").upper()
                            vol = _safe_float(it.get("volume"), 0.0)
                            if sym:
                                backpack_spot[sym] = vol
            except Exception as e:
                Logger.error(f"Backpack SPOT load error: {e}")

            try:
                async with session.get(MARKET_TICKER_ENDPOINTS["BACKPACK_FUT"]) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        for it in j:
                            sym = str(it.get("symbol") or "").upper()
                            vol = _safe_float(it.get("volume"), 0.0)
                            if sym:
                                backpack_fut[sym] = vol
            except Exception as e:
                Logger.error(f"Backpack FUTURES load error: {e}")

        for sym in binance_usdt_symbols:
            sym_up = str(sym).upper()
            base = _base_from_binance_usdt_perp(sym_up)
            candidates: list[dict] = []

            # BYBIT USDT
            t = _tiger_symbol("BYBIT", "SPOT", base, "USDT")
            v = bybit_spot.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "BYBIT", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDT"})
            t = _tiger_symbol("BYBIT", "FUTURES", base, "USDT")
            v = bybit_fut.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "BYBIT", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDT"})

            # OKX USDT
            t = _tiger_symbol("OKX", "SPOT", base, "USDT")
            v = okx_spot.get(f"{base}-USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "OKX", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDT"})
            t = _tiger_symbol("OKX", "FUTURES", base, "USDT")
            v = okx_swap.get(f"{base}-USDT-SWAP", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "OKX", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDT"})

            # MEXC SPOT USDT
            t = _tiger_symbol("MEXC", "SPOT", base, "USDT")
            v = mexc_spot.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "MEXC", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDT"})

            # Gate.io BTC_USDT
            t = _tiger_symbol("GATE.IO", "SPOT", base, "USDT")
            v = gate_spot.get(f"{base}_USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "GATE.IO", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDT"})
            t = _tiger_symbol("GATE.IO", "FUTURES", base, "USDT")
            v = gate_fut.get(f"{base}_USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "GATE.IO", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDT"})

            # Bitget USDT
            t = _tiger_symbol("BITGET", "SPOT", base, "USDT")
            v = bitget_spot.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "BITGET", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDT"})
            t = _tiger_symbol("BITGET", "FUTURES", base, "USDT")
            v = bitget_fut.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "BITGET", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDT"})

            # AsterDex USDT
            t = _tiger_symbol("ASTERDEX", "SPOT", base, "USDT")
            v = aster_spot.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "ASTERDEX", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDT"})
            t = _tiger_symbol("ASTERDEX", "FUTURES", base, "USDT")
            v = aster_fut.get(f"{base}USDT", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "ASTERDEX", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDT"})

            # Upbit KRW-BTC
            t = _tiger_symbol("UPBIT", "SPOT", base, "KRW")
            v = upbit.get(f"KRW-{base}", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "UPBIT", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "KRW"})

            # Hyperliquid BTC
            t = _tiger_symbol("HYPERLIQUID", "FUTURES", base, None)
            v = hyper.get(base, 0.0)
            if t and v > 0:
                candidates.append({"exchange": "HYPERLIQUID", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDC"})

            # Backpack BTC_USDC
            t = _tiger_symbol("BACKPACK", "SPOT", base, "USDC")
            v = backpack_spot.get(f"{base}_USDC", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "BACKPACK", "market": "SPOT", "tiger": t, "volume_usd": v, "quote": "USDC"})
            t = _tiger_symbol("BACKPACK", "FUTURES", base, "USDC")
            v = backpack_fut.get(f"{base}_USDC_PERP", 0.0)
            if t and v > 0:
                candidates.append({"exchange": "BACKPACK", "market": "FUTURES", "tiger": t, "volume_usd": v, "quote": "USDC"})

            candidates.sort(key=lambda x: float(x.get("volume_usd") or 0.0), reverse=True)
            if EXTERNAL_MARKETS_MAX_PER_SYMBOL and len(candidates) > EXTERNAL_MARKETS_MAX_PER_SYMBOL:
                candidates = candidates[:EXTERNAL_MARKETS_MAX_PER_SYMBOL]

            out[sym_up] = candidates

        return out

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



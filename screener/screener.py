import asyncio
import time
from unittest import result
from screener.impulses import ImpulseDetector
from screener.ws_manager import WSManager
from screener.symbol_fetcher import SymbolFetcher
from notifier import Notifier
from logger import Logger
from datetime import datetime
from screener.clusters import ClusterManager
from config import ENABLE_ATR_IMPULSE, ENABLE_MARK_DELTA
from screener.signal_hub import SignalHub
import math
from users_store import UsersStore

def fmt_compact_usdt(x: float) -> str:
    try:
        x = float(x or 0)
    except Exception:
        return "0"

    absx = abs(x)

    def _fmt(val: float, suffix: str, dec: int):
        s = f"{val:.{dec}f}"
        # убираем .0
        if s.endswith(".0"):
            s = s[:-2]
        # русская запятая
        s = s.replace(".", ",")
        return f"{s}{suffix}"

    if absx >= 1_000_000_000:
        val = x / 1_000_000_000
        return _fmt(val, "B", 1)  # 1,5B
    if absx >= 1_000_000:
        val = x / 1_000_000
        return _fmt(val, "M", 1)  # 1,2M
    if absx >= 1_000:
        val = x / 1_000
        # K без десятых, как ты просил (300K)
        return _fmt(val, "K", 0)
    return f"{int(x)}"

def fmt_signed_pct(x: float, decimals: int = 3) -> str:
    try:
        x = float(x)
    except Exception:
        return "0%"
    s = f"{x:+.{decimals}f}".replace(".", ",")
    return f"{s}%"

def dyn_threshold(volume: float, v_min: float, v_max: float, p_min: float, p_max: float, exponent: float) -> float:
    # защита от мусора
    volume = float(volume or 0.0)
    v_min = max(float(v_min or 1.0), 1.0)
    v_max = max(float(v_max or v_min), v_min + 1.0)
    p_min = float(p_min or 0.5)
    p_max = float(p_max or 5.0)
    exponent = float(exponent or 0.8)

    x = min(max(volume, v_min), v_max)
    norm = (math.log10(x) - math.log10(v_min)) / (math.log10(v_max) - math.log10(v_min))
    factor = max(0.0, min(1.0, norm)) ** exponent
    percent = p_max - (p_max - p_min) * factor
    return float(percent)

def user_match_impulse(user_cfg: dict, payload: dict, vol24h: float, trades24h: int, ob: dict) -> bool:
    # exclude
    excl = set((user_cfg.get("exclude_symbols") or []))
    if payload.get("symbol", "").upper() in excl:
        return False

    # enable blocks
    atr_enabled = bool((user_cfg.get("atr_impulse") or {}).get("enabled", True))
    mark_enabled = bool((user_cfg.get("mark_delta") or {}).get("enabled", True))

    reason = set(payload.get("reason") or [])
    has_mark = ("mark_delta" in reason) and (payload.get("mark_delta_pct") is not None)

    # Если ATR выключен, а mark включен — пропускаем только события, где реально есть mark_delta
    if (not atr_enabled) and mark_enabled:
        if not has_mark:
            return False

    # Если mark выключен — не требуем mark_delta
    # Если оба выключены — смысла нет
    if (not atr_enabled) and (not mark_enabled):
        return False

    # volume / trades24h / orderbook — только ужесточение (вариант A)
    v_thr = float(user_cfg.get("volume_threshold") or 20_000_000)
    if vol24h < v_thr:
        return False

    t_thr = int(user_cfg.get("min_trades_24h") or 10_000)
    if trades24h < t_thr:
        return False

    ob_bid_thr = float(user_cfg.get("orderbook_min_bid") or 20_000)
    ob_ask_thr = float(user_cfg.get("orderbook_min_ask") or 20_000)
    if float((ob or {}).get("bid", 0)) < ob_bid_thr:
        return False
    if float((ob or {}).get("ask", 0)) < ob_ask_thr:
        return False

    # impulse filters (тоже ужесточение)
    imp = user_cfg.get("impulse") or {}

    impulse_min_trades = int(imp.get("impulse_min_trades") or 1000)
    if int(payload.get("impulse_trades") or 0) < impulse_min_trades:
        return False

    # динамический порог по % для юзера
    p_min = float(imp.get("p_min") or 0.5)
    p_max = float(imp.get("p_max") or 5.0)
    exponent = float(imp.get("exponent") or 0.8)
    # v_min = user volume_threshold, v_max фикс 5B
    user_thr = dyn_threshold(vol24h, v_thr, 5_000_000_000, p_min, p_max, exponent)
    if float(payload.get("change_percent") or 0.0) < user_thr:
        return False

    # ATR multiplier фильтруем по atr_impulse (амплитуда в ATR)
    atr_mult = float(imp.get("atr_multiplier") or 2.0)
    if float(payload.get("atr_impulse") or 0.0) < atr_mult:
        return False

    # mark_delta pct фильтруем, если включено
    if mark_enabled:
        md = user_cfg.get("mark_delta") or {}
        md_thr = float(md.get("pct") or 1.0)
        md_val = payload.get("mark_delta_pct")
        if md_val is None or abs(float(md_val)) < md_thr:
            return False

    return True

class ATRImpulseScreener:
    def __init__(self):
        self.notifier = Notifier()
        self.last_alert_time = {}
        self.symbol_thresholds = {}
        self.cluster_mgr = ClusterManager()
        self.users = UsersStore("users.json")
        # active impulse session per symbol
        self.impulse_sessions = {}  # symbol -> session dict

        self.impulse_detector = ImpulseDetector()
        self.ws_manager = WSManager(self.handle_trade)
        self.ws_manager.set_mark_handler(self.handle_mark)
        self.symbol_fetcher = SymbolFetcher()
        self.last_price = {}
        self.mark_price = {}
        self.signal_hub = None
        self._signalhub_server = None
        # Чтобы отслеживать активные WS-задания
        self.active_ws_tasks = {}


    async def handle_trade(self, symbol, data):
        price = float(data.get("p", 0))
        qty   = float(data.get("q", 0))
        ts    = time.time()

        self.last_price[symbol] = price

        # ЕДИНСТВЕННОЕ место, где обновляется "история"
        self.cluster_mgr.add_tick(symbol, ts, price, qty)

        threshold = self.symbol_thresholds.get(symbol.lower(), 1.0)

        # ==============================
        #  IMPULSE SESSION LAYER
        # ==============================
        sess = self.impulse_sessions.get(symbol)

        # 1) Если сессия уже активна — обновляем максимум и пытаемся доставить
        if sess is not None:
            if self._session_expired(sess, ts):
                try:
                    dur = max(ts - float(sess.get("ref_time") or ts), 0.0)
                    mx = float(sess.get("max_change_percent") or 0.0)
                    Logger.success(f"⚡ Impulse session END: {symbol.upper()} | dur={dur:.2f}s | max={mx:.2f}%")
                except Exception:
                    Logger.success(f"⚡ Impulse session END: {symbol.upper()}")
                self.impulse_sessions.pop(symbol, None)
                return


            self._update_session_metrics(symbol, sess, ts)
            await self._deliver_session_to_users(symbol, sess, ts)
            return  # важно: не запускаем детектор заново

        # 2) Сессии нет — проверяем старт импульса детектором (как раньше)
        result = None
        if ENABLE_ATR_IMPULSE:
            result = await self.impulse_detector.check_atr_impulse(
                symbol=symbol,
                cluster_mgr=self.cluster_mgr,
                last_alert_time=self.last_alert_time,   # антиспам только на старт
                symbol_threshold=threshold,
                last_price_map=self.last_price,
                mark_price_map=self.mark_price,
            )

        if not result:
            return

        # 3) Старт новой сессии
        symbol_up = symbol.upper()

        ref_time = float(result["ref_time"])
        ref_price = float(result["ref_price"])

        sess = {
            "ref_time": ref_time,
            "ref_price": ref_price,

            # стартовые значения
            "max_change_percent": float(result.get("change_percent") or 0.0),
            "max_price": float(result.get("cur") or price),
            "cur_price": float(result.get("cur") or price),

            "impulse_trades": int(result.get("impulse_trades") or 0),
            "impulse_volume_usdt": float(result.get("impulse_volume_usdt") or 0.0),

            # max ATR impulse стартуем с 0 и обновим ниже
            "max_atr_impulse": 0.0,

            # mark (если есть)
            "mark_delta_pct": result.get("mark_delta_pct"),
            "mark_extreme": result.get("mark_extreme"),

            "reason": result.get("reason") or ["atr"],
            "sent_to_users": set(),
        }
        self.impulse_sessions[symbol] = sess

        # антиспам — только на старт сессии
        self.last_alert_time[symbol] = time.time()

        Logger.success(
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ⚡ Impulse session START: {symbol_up}"
        )

        # обновим метрики на текущем тике и попробуем доставить
        self._update_session_metrics(symbol, sess, ts)
        await self._deliver_session_to_users(symbol, sess, ts)
        return


    async def run(self):
        await self.notifier.start()
        await self.notifier.send_message("✅ ATR-скринер запущен.")

        self.signal_hub = SignalHub(
            auth_resolver=self.users.resolve_token,
            config_getter_for_user=self.users.get_user_cfg,
            config_patcher_for_user=self.users.patch_user_cfg,
            top_provider=self._get_top
        )
        self._signalhub_server = await self.signal_hub.start()
        self.notifier.set_signal_hub(self.signal_hub)


        try:
            while True:
                symbols_24h_volume = await self.symbol_fetcher.fetch_futures_symbols()

                # сохраняем 24h объём
                self.symbol_24h_volume = symbols_24h_volume
                self.symbol_thresholds = symbols_24h_volume["thresholds"]

                # создаём список символов
                symbols = list(symbols_24h_volume["volumes"].keys())

                # сортировка: объём от большего к меньшему
                symbols.sort(key=lambda s: symbols_24h_volume["volumes"][s], reverse=True)

                Logger.info(f"Всего символов после фильтров: {len(symbols)}")
                Logger.info("Символ — Объём — Threshold:")

                for s in symbols:
                    vol = symbols_24h_volume["volumes"][s]
                    th = symbols_24h_volume["thresholds"][s]
                    Logger.info(f"{s.upper()}: {vol:,.0f} USDT — порог {th}%")

                Logger.info(f"Всего символов после фильтров: {len(symbols)}")
                #Logger.info(f"Символы:\n{', '.join(symbols)}")

                # Запуск WS для новых символов
                for symbol in symbols:
                    if symbol not in self.active_ws_tasks:
                        Logger.info(f"Запущен WebSocket для {symbol}")
                        self.ws_manager.start_task(symbol)
                        self.active_ws_tasks[symbol] = True

                # Остановка WS для неактивных символов
                to_remove = [s for s in self.active_ws_tasks if s not in symbols]
                for s in to_remove:
                    self.ws_manager.stop_task(s)
                    del self.active_ws_tasks[s]

                await asyncio.sleep(3600)

        finally:
            # если run() отменили (Ctrl+C) — всё аккуратно закрываем
            await self.close()

    async def close(self):
        # стопнуть все binance ws таски
        for s in list(self.active_ws_tasks.keys()):
            self.ws_manager.stop_task(s)
            self.active_ws_tasks.pop(s, None)

        # закрыть signalhub server
        if self._signalhub_server is not None:
            self._signalhub_server.close()
            await self._signalhub_server.wait_closed()
            self._signalhub_server = None

        # закрыть aiohttp сессию телеги
        await self.notifier.close()
        
    async def handle_mark(self, symbol, data):
        if not ENABLE_MARK_DELTA:
            return
        mp = float(data.get("p", 0))
        if mp:
            self.mark_price[symbol] = mp
            self.cluster_mgr.add_mark(symbol, time.time(), mp)


    def _get_runtime_config(self):
        from config import (
            IMPULSE_MAX_LOOKBACK, IMPULSE_MIN_LOOKBACK, IMPULSE_MIN_TRADES,
            CLUSTER_INTERVAL, MARK_DELTA_PCT, ENABLE_ATR_IMPULSE, ENABLE_MARK_DELTA
        )
        return {
            "IMPULSE_MAX_LOOKBACK": IMPULSE_MAX_LOOKBACK,
            "IMPULSE_MIN_LOOKBACK": IMPULSE_MIN_LOOKBACK,
            "IMPULSE_MIN_TRADES": IMPULSE_MIN_TRADES,
            "CLUSTER_INTERVAL": CLUSTER_INTERVAL,
            "MARK_DELTA_PCT": MARK_DELTA_PCT,
            "ENABLE_ATR_IMPULSE": ENABLE_ATR_IMPULSE,
            "ENABLE_MARK_DELTA": ENABLE_MARK_DELTA,
        }

    def _patch_runtime_config(self, patch: dict):
        import config as C
        allow = set(self._get_runtime_config().keys())
        for k, v in (patch or {}).items():
            if k in allow:
                setattr(C, k, v)
        return self._get_runtime_config()

    async def _get_top(self, mode: str, n: int):
        if not hasattr(self, "symbol_24h_volume") or not self.symbol_24h_volume:
            return []
        vols = self.symbol_24h_volume.get("volumes", {})
        items = sorted(vols.items(), key=lambda x: x[1], reverse=True)[:n]
        return [{"symbol": s.upper(), "value": float(v)} for s, v in items]

    def _session_expired(self, sess: dict, now_ts: float) -> bool:
        from config import IMPULSE_MAX_LOOKBACK
        return (now_ts - float(sess["ref_time"])) > float(IMPULSE_MAX_LOOKBACK)


    def _update_session_metrics(self, symbol: str, sess: dict, now_ts: float) -> None:
        """
        Обновляем максимум/метрики текущей импульс-сессии.
        Детектор НЕ вызываем. Просто обновляем max и stats.
        """
        ref_price = float(sess.get("ref_price") or 0.0)
        cur_price = float(self.last_price.get(symbol) or 0.0)
        if ref_price <= 0 or cur_price <= 0:
            return

        # max % от ref_price по текущей цене
        cur_change = abs(cur_price - ref_price) / ref_price * 100.0
        if cur_change > float(sess.get("max_change_percent") or 0.0):
            sess["max_change_percent"] = float(cur_change)
            sess["max_price"] = float(cur_price)
            sess["cur_price"] = float(cur_price)  # чтобы в сообщении "цена на момент отправки"

        # stats по окну ref..now
        tr, vol = self.cluster_mgr.get_impulse_stats(symbol, float(sess["ref_time"]), now_ts)
        sess["impulse_trades"] = int(tr)
        sess["impulse_volume_usdt"] = float(vol)

        # max ATR impulse
        atr = float(self.cluster_mgr.get_atr(symbol) or 0.0)

        if atr <= 0:
            # ATR ещё не успел посчитаться (нет свечей) — не душим импульсы
            atr_imp = float("inf")
        else:
            atr_imp = abs(cur_price - ref_price) / atr

        if atr_imp > float(sess.get("max_atr_impulse") or 0.0):
            sess["max_atr_impulse"] = float(atr_imp)


        # mark extreme (если включено)
        from config import ENABLE_MARK_DELTA
        if ENABLE_MARK_DELTA:
            me = self.cluster_mgr.get_mark_last_delta_extreme(symbol, float(sess["ref_time"]), now_ts)
            sess["mark_extreme"] = me
            sess["mark_delta_pct"] = (me["delta"] if me else None)


    async def _deliver_session_to_users(self, symbol: str, sess: dict, ts: float) -> None:
        """
        Отправляем событие тем пользователям, чей фильтр проходит
        ПО ТЕКУЩЕМУ MAX сессии. Одному user_id — максимум 1 раз за сессию.
        """
        symbol_up = symbol.upper()

        # метрики символа из symbol_fetcher
        vol24h = float(self.symbol_24h_volume["volumes"].get(symbol.lower(), 0))
        trades24h = int((self.symbol_24h_volume.get("trades24h") or {}).get(symbol.lower(), 0))
        ob = (self.symbol_24h_volume.get("orderbook") or {}).get(symbol.lower(), {}) or {}

        payload = {
            "type": "impulse",
            "exchange": "BINANCE-FUT",
            "market": "FUTURES",
            "symbol": symbol_up,
            "change_percent": float(sess.get("max_change_percent") or 0.0),
            "impulse_trades": int(sess.get("impulse_trades") or 0),
            "impulse_volume_usdt": float(sess.get("impulse_volume_usdt") or 0.0),
            "atr_impulse": float(sess.get("max_atr_impulse") or 0.0),
            "mark_delta_pct": sess.get("mark_delta_pct"),
            "mark_extreme": sess.get("mark_extreme"),
            "ts": float(ts),
            "reason": sess.get("reason") or ["atr"],
        }

        if not sess.get("admin_sent"):
            try:
                await self.notifier.send_message(message)  # chat_id=None -> default_chat_id (админ)
                Logger.info(f"ADMIN notify: {symbol_up} (session max {payload['change_percent']:.2f}%)")
            except Exception as e:
                Logger.error(f"ADMIN notify error: {e}")
            sess["admin_sent"] = True

        sent = sess.setdefault("sent_to_users", set())

        # подготовим “красивое” сообщение (как у тебя), но по данным сессии
        # Важно: ref_price/ref_time фикс, а “цена срабатывания” = цена в момент отправки
        from datetime import datetime
        now = float(ts)
        ref_time = float(sess["ref_time"])
        ref_price = float(sess["ref_price"])
        cur_price = float(sess.get("cur_price") or self.last_price.get(symbol) or 0.0)

        change_percent = float(payload["change_percent"])
        duration = max(now - ref_time, 0.001)
        speed_percent = change_percent / duration

        atr_value = float(self.cluster_mgr.get_atr(symbol) or 0.0)
        atr_impulse = float(payload["atr_impulse"])

        # direction берём по текущей цене
        direction = (cur_price - ref_price)
        color = "🟢" if direction > 0 else "🔴"
        direction_text = "Памп" if direction > 0 else "Дамп"

        # mark block
        mark_block = ""
        from config import ENABLE_MARK_DELTA
        if ENABLE_MARK_DELTA:
            mark_trigger = payload.get("mark_delta_pct")
            mark_extreme = payload.get("mark_extreme")
            if mark_trigger is not None:
                mark_block += f"🧷 Δ Mark-Last (текущий экстремум): {fmt_signed_pct(mark_trigger)}\n"
            if mark_extreme:
                mark_block += (
                    f"📈 Δ Mark-Last max (сессия): {fmt_signed_pct(mark_extreme['delta'])} "
                    f"(mark updates: {mark_extreme['mark_updates']})\n"
                )

        message = (
            f"{color} <code>{symbol_up}</code> {direction_text}\n"
            f"Max изменение: {change_percent:.2f}% за {duration:.2f} сек\n\n"
            f"📍 Начальная цена импульса: {ref_price}\n"
            f"🚀 Цена (момент отправки): {cur_price}\n\n"
            f"{mark_block}\n"
            f"Скорость: {speed_percent:.3f}%/сек\n"
            f"📐 Амплитуда импульса: {atr_impulse:.2f} ATR\n"
            f"📊 Объём 24ч: {fmt_compact_usdt(vol24h)} USDT\n"
            f"🔥 Объём за импульс: {fmt_compact_usdt(payload['impulse_volume_usdt'])} USDT "
            f"({payload['impulse_trades']} сделок)"
        )

        # рассылка по пользователям
        for uid, user in self.users.all_users().items():
            if uid in sent:
                continue

            if not user_match_impulse(user.cfg, payload, vol24h, trades24h, ob):
                continue

            # WS
            if self.signal_hub:
                await self.signal_hub.send_to_user(uid, payload)

            # Telegram
            if user.tg_chat_id:
                await self.notifier.send_message(message, chat_id=user.tg_chat_id)
                
            Logger.info(
                f"DELIVER impulse: {symbol_up} -> user={uid} "
                f"(tg={'yes' if user.tg_chat_id else 'no'}, ws={'yes' if self.signal_hub else 'no'}) "
                f"max={payload['change_percent']:.2f}% trades={payload['impulse_trades']}"
            )

            sent.add(uid)


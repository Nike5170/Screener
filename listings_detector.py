# listings_detector.py

import asyncio
import aiohttp
from logger import Logger
from notifier import Notifier


class ListingsDetector:
    def __init__(self, notifier: Notifier):
        self.notifier = notifier
        self.known_symbols = None

    async def detect(self):
        """Основной цикл контроля листингов"""
        url_info = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        timeout = aiohttp.ClientTimeout(total=5)

        while True:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url_info) as resp:
                        if resp.status != 200:
                            Logger.warn(f"HTTP error {resp.status} при загрузке exchangeInfo")
                            await asyncio.sleep(20)
                            continue

                        data = await resp.json()

                current = {
                    s["symbol"].lower()
                    for s in data["symbols"]
                    if s["contractType"] == "PERPETUAL"
                    and s["quoteAsset"] == "USDT"
                    and s["status"] == "TRADING"
                }

                # Первичная инициализация
                if self.known_symbols is None:
                    self.known_symbols = current
                    Logger.success(f"ListingDetector инициализирован ({len(current)} symbols)")
                    await asyncio.sleep(20)
                    continue

                # Поиск нового листинга
                new_listed = current - self.known_symbols

                for symbol in new_listed:
                    Logger.info(f"🆕 Листинг обнаружен: {symbol.upper()}")

                    await self.notifier.send_message(
                        f"🆕 <b>Листинг на Binance Futures</b>\n"
                        f"<code>{symbol.upper()}</code>"
                    )

                    await self.notifier.send_clipboard(symbol.upper())

                self.known_symbols |= new_listed

            except Exception as e:
                Logger.error(f"Ошибка в ListingsDetector: {e}")

            await asyncio.sleep(20)

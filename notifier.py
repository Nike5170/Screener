# notifier.py
import asyncio
import aiohttp
import socket
from logger import Logger
from config import (
    TELEGRAM_TOKEN,
    TELEGRAM_CHAT_IDS,
    CLIPBOARD_HOSTS,
    CLIPBOARD_PORT,
    CLIPBOARD_CONNECT_ATTEMPTS,
    CLIPBOARD_RETRY_BACKOFF
)


class Notifier:
    def __init__(self):
        self.TG_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

        self.chat_ids = TELEGRAM_CHAT_IDS
        self.queue = asyncio.Queue()

        # Clipboard
        self.clipboard_hosts = CLIPBOARD_HOSTS
        self.clipboard_port = CLIPBOARD_PORT
        self.clipboard_connections = {}


    async def init_clipboard(self):
        for host in self.clipboard_hosts:
            for attempt in range(1, CLIPBOARD_CONNECT_ATTEMPTS + 1):
                try:
                    Logger.info(f"Подключаемся к {host}:{self.clipboard_port} (попытка {attempt})")
                    _, writer = await asyncio.open_connection(host, self.clipboard_port)
                    self.clipboard_connections[host] = writer
                    Logger.success(f"Соединение установлено с {host}")
                    break
                except Exception as e:
                    Logger.error(f"Ошибка подключения: {e}")
                    await asyncio.sleep(CLIPBOARD_RETRY_BACKOFF ** attempt)


    async def start(self):
        # 3 воркера → параллельная отправка без задержек
        for _ in range(3):
            asyncio.create_task(self.worker())


    async def send_message(self, text: str):
        await self.queue.put(("telegram", text))


    async def send_clipboard(self, text: str):
        await self.queue.put(("clipboard", text))


    async def worker(self):
        while True:
            channel, text = await self.queue.get()

            if channel == "telegram":
                await self._send_telegram(text)
            else:
                await self._send_clipboard(text)

            self.queue.task_done()


    async def _send_telegram(self, message):
        payloads = [
            {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
            for chat_id in self.chat_ids
        ]

        for payload in payloads:
            for attempt in range(1, 5):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(self.TG_URL, json=payload) as resp:

                            if resp.status == 200:
                                Logger.info(f"📨 Telegram → OK ({payload['chat_id']})")
                                break

                            Logger.error(
                                f"⚠ Telegram status {resp.status}: {await resp.text()}"
                            )
                            await asyncio.sleep(0.2)

                except asyncio.TimeoutError:
                    Logger.error(f"⏳ Timeout → retry {attempt}")
                    await asyncio.sleep(0.2)

                except Exception as e:
                    Logger.error(f"❌ Ошибка Telegram: {e}")
                    await asyncio.sleep(0.25)



    async def _send_clipboard(self, message):
        for host in self.clipboard_hosts:
            try:
                writer = self.clipboard_connections.get(host)

                if writer is None or writer.is_closing():
                    Logger.error(f"Соединение с {host} отсутствует → переподключаемся...")
                    await self._reconnect(host)
                    writer = self.clipboard_connections.get(host)

                    if writer is None:
                        Logger.error(f"❌ Не удалось восстановить соединение ({host})")
                        continue

                writer.write((message + "\n").encode("utf-8"))
                await writer.drain()
                Logger.info(f"📋 Clipboard → '{message}'")

            except Exception as e:
                Logger.error(f"Ошибка отправки: {e}")
                await self._reconnect(host)


    async def _reconnect(self, host):
        try:
            Logger.info(f"🔄 Переподключение {host}...")
            _, writer = await asyncio.open_connection(host, self.clipboard_port)
            self.clipboard_connections[host] = writer
            Logger.success(f"🔗 Соединение восстановлено: {host}")

        except Exception as e:
            Logger.error(f"❌ Не удалось восстановить соединение с {host}: {e}")
            try:
                self.clipboard_connections[host].close()
            except:
                pass
            self.clipboard_connections.pop(host, None)

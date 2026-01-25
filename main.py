import asyncio

from screener.screener import ATRImpulseScreener
from logger import Logger

async def main():
    Logger.success("Запуск ATR-скринера...")
    screener = ATRImpulseScreener()
    await screener.run()

if __name__ == "__main__":
    asyncio.run(main())

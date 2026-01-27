import asyncio

from screener.screener import ATRImpulseScreener
from logger import Logger

async def main():
    Logger.success("Запуск ATR-скринера...")
    screener = ATRImpulseScreener()
    try:
        await screener.run()
    finally:
        # на случай если вылетели до finally внутри run()
        await screener.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

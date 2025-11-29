from datetime import datetime

class Logger:
    PREFIX = {
        "info": "",
        "warn": "⚠️ ",
        "error": "❌ ",
        "success": "✅ ",
        "debug": "🔍 "
    }

    @staticmethod
    def ts():
        return datetime.now().strftime('%H:%M:%S.%f')[:-3]

    @staticmethod
    def log(level, msg):
        prefix = Logger.PREFIX.get(level, "")
        print(f"[{Logger.ts()}] {prefix}{msg}")

    @staticmethod
    def info(msg):    Logger.log("info", msg)
    @staticmethod
    def warn(msg):    Logger.log("warn", msg)
    @staticmethod
    def error(msg):   Logger.log("error", msg)
    @staticmethod
    def success(msg): Logger.log("success", msg)
    @staticmethod
    def debug(msg):   Logger.log("debug", msg)

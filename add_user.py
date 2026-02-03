# add_user.py
import argparse
from users_store import UsersStore

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uid", required=True, help="user_id, например u1")
    ap.add_argument("--tg", default=None, help="Telegram chat_id (можно пусто)")
    ap.add_argument("--token", default=None, help="Задать токен вручную (опционально)")
    ap.add_argument("--overwrite", action="store_true", help="Перезаписать пользователя если существует")
    args = ap.parse_args()

    store = UsersStore("users.json")

    rec = store.create_user(
        user_id=args.uid,
        tg_chat_id=args.tg,
        token=args.token,
        overwrite=args.overwrite,
    )

    print("OK: user created/updated")
    print("user_id:", args.uid)
    print("token:", rec["token"])
    print("tg_chat_id:", rec.get("tg_chat_id"))

if __name__ == "__main__":
    main()

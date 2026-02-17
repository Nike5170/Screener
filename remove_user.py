# remove_user.py
import argparse
from users_store import UsersStore

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uid", required=True, help="user_id to remove")
    args = ap.parse_args()

    store = UsersStore("users.json")
    users = store._data.get("users", {})

    if args.uid not in users:
        print(f"user not found: {args.uid}")
        return

    del users[args.uid]
    store.save()
    print(f"OK: user removed: {args.uid}")

if __name__ == "__main__":
    main()

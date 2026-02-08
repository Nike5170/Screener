# users_store.py
import copy
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

DEFAULT_USERS_PATH = "users.json"

# Плоские ключи как в users.json -> filters
ALLOWED_FILTERS: Dict[str, list] = {
    "volume_threshold": [10_000_000, 50_000_000, 100_000_000, 200_000_000, 500_000_000],
    "min_trades_24h": [10_000, 50_000, 100_000, 200_000],
    "orderbook_min_bid": [20_000, 50_000, 100_000, 200_000],
    "orderbook_min_ask": [20_000, 50_000, 100_000, 200_000],
    "impulse_min_trades": [100, 500, 1000],
    "mark_delta_pct": [0.5, 1.0, 2.0],
}


def _now() -> float:
    return time.time()


def _normalize_num(x):
    if isinstance(x, bool):
        return x
    try:
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return x
        if isinstance(x, str):
            s = x.strip()
            if "." in s or "e" in s.lower():
                return float(s)
            return int(s)
    except Exception:
        pass
    return x


def _default_filters_from_allowed() -> Dict[str, Any]:
    """
    Дефолты = первые значения из ALLOWED_FILTERS,
    чтобы не дублировать дефолты вручную.
    """
    out: Dict[str, Any] = {}
    for k, arr in ALLOWED_FILTERS.items():
        if arr:
            out[k] = _normalize_num(arr[0])
    return out


def _validate_patch(patch: dict) -> dict:
    """
    Разрешаем менять только ключи из ALLOWED_FILTERS и только на значения из списка.
    Остальное игнорируем.
    """
    out: Dict[str, Any] = {}
    for k, v in (patch or {}).items():
        k = str(k)
        if k not in ALLOWED_FILTERS:
            continue

        v_norm = _normalize_num(v)
        allowed = ALLOWED_FILTERS[k]

        ok = False
        for a in allowed:
            a_norm = _normalize_num(a)
            if isinstance(a_norm, (int, float)) and isinstance(v_norm, (int, float)):
                if float(a_norm) == float(v_norm):
                    ok = True
                    v_norm = a_norm
                    break
            else:
                if a_norm == v_norm:
                    ok = True
                    v_norm = a_norm
                    break

        if ok:
            out[k] = v_norm

    return out


@dataclass
class UserProfile:
    user_id: str
    token: str
    tg_chat_id: Optional[str]
    cfg: Dict[str, Any]


class UsersStore:
    """
    Всегда возвращаем cfg уже смерженный с дефолтами.
    Тогда в коде проверок не нужны `or 20000` и т.п.
    """

    def __init__(self, path: str = DEFAULT_USERS_PATH):
        self.path = path
        self._data: Dict[str, Any] = {"users": {}}
        self._default_filters = _default_filters_from_allowed()
        self.load()

    def load(self):
        if not os.path.exists(self.path):
            self._data = {"users": {}}
            self.save()
            return
        with open(self.path, "r", encoding="utf-8") as f:
            self._data = json.load(f) or {"users": {}}
        if "users" not in self._data:
            self._data["users"] = {}

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def _merge_defaults(self, filters: dict | None) -> Dict[str, Any]:
        cfg = copy.deepcopy(self._default_filters)
        if isinstance(filters, dict):
            for k, v in filters.items():
                cfg[str(k)] = v
        return cfg

    def all_users(self) -> Dict[str, UserProfile]:
        out: Dict[str, UserProfile] = {}
        for uid, u in (self._data.get("users") or {}).items():
            out[uid] = UserProfile(
                user_id=uid,
                token=str(u.get("token") or ""),
                tg_chat_id=str(u.get("tg_chat_id")) if u.get("tg_chat_id") else None,
                cfg=self._merge_defaults(u.get("filters") or {}),
            )
        return out

    def resolve_token(self, token: str) -> Optional[str]:
        token = str(token or "")
        if not token:
            return None
        for uid, u in (self._data.get("users") or {}).items():
            if str(u.get("token") or "") == token:
                return uid
        return None

    def get_user_cfg(self, user_id: str) -> Dict[str, Any]:
        u = (self._data.get("users") or {}).get(user_id) or {}
        return self._merge_defaults(u.get("filters") or {})

    def patch_user_cfg(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        users = self._data.setdefault("users", {})
        u = users.setdefault(user_id, {})
        cur = u.setdefault("filters", {})

        safe_patch = _validate_patch(patch or {})
        cur.update(safe_patch)

        u["updated_at"] = _now()
        self.save()

        # UI пусть получает полный конфиг (с дефолтами тоже)
        return self._merge_defaults(cur)

    def create_user(
        self,
        user_id: str,
        tg_chat_id: str | None = None,
        token: str | None = None,
        filters: dict | None = None,
        overwrite: bool = False,
    ) -> dict:
        import secrets

        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is empty")

        users = self._data.setdefault("users", {})

        if (user_id in users) and (not overwrite):
            raise ValueError(f"user_id already exists: {user_id}")

        if token is None:
            token = secrets.token_urlsafe(24)

        final_filters = self._merge_defaults(filters or {})

        users[user_id] = {
            "token": token,
            "tg_chat_id": str(tg_chat_id) if tg_chat_id else None,
            "filters": final_filters,
            "created_at": _now(),
            "updated_at": _now(),
        }
        self.save()
        return users[user_id]

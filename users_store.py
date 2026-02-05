# users_store.py
import json
import os
from shutil import copy
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
import copy

DEFAULT_USERS_PATH = "users.json"

ALLOWED_FILTERS = {
    "volume_threshold": [20e6, 50e6, 100e6, 200e6, 500e6],
    "min_trades_24h": [10_000, 50_000, 100_000, 200_000],
    "orderbook_min_bid": [20_000, 50_000, 100_000, 200_000],
    "orderbook_min_ask": [20_000, 50_000, 100_000, 200_000],
    "impulse.impulse_min_trades": [1000, 2000],
    "mark_delta.pct": [0.5, 1.0, 2.0],
}
def _now() -> float:
    return time.time()


@dataclass
class UserProfile:
    user_id: str
    token: str
    tg_chat_id: Optional[str]
    cfg: Dict[str, Any]


class UsersStore:
    """
    Вариант A:
      - сервер работает по минимальным параметрам
      - user cfg может только ужесточать фильтры
    """

    def __init__(self, path: str = DEFAULT_USERS_PATH):
        self.path = path
        self._data: Dict[str, Any] = {"users": {}}
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

    def all_users(self) -> Dict[str, UserProfile]:
        out: Dict[str, UserProfile] = {}
        for uid, u in (self._data.get("users") or {}).items():
            out[uid] = UserProfile(
                user_id=uid,
                token=str(u.get("token") or ""),
                tg_chat_id=str(u.get("tg_chat_id")) if u.get("tg_chat_id") else None,
                cfg=(u.get("filters") or {}),
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

    def get_user(self, user_id: str) -> Optional[UserProfile]:
        u = (self._data.get("users") or {}).get(user_id)
        if not u:
            return None
        return UserProfile(
            user_id=user_id,
            token=str(u.get("token") or ""),
            tg_chat_id=str(u.get("tg_chat_id")) if u.get("tg_chat_id") else None,
            cfg=(u.get("filters") or {}),
        )

    def get_user_cfg(self, user_id: str) -> Dict[str, Any]:
        u = (self._data.get("users") or {}).get(user_id) or {}
        return u.get("filters") or {}

    def patch_user_cfg(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        users = self._data.setdefault("users", {})
        u = users.setdefault(user_id, {})
        cur = u.setdefault("filters", {})

        safe_patch = _validate_patch(patch or {})

        def deep_merge(dst: Dict[str, Any], src: Dict[str, Any]):
            for k, v in src.items():
                if isinstance(v, dict) and isinstance(dst.get(k), dict):
                    deep_merge(dst[k], v)
                else:
                    dst[k] = v

        deep_merge(cur, safe_patch)
        u["updated_at"] = _now()
        self.save()
        return cur

    
    def create_user(
        self,
        user_id: str,
        tg_chat_id: str | None = None,
        token: str | None = None,
        filters: dict | None = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Создаёт/обновляет пользователя в users.json.
        Возвращает созданную запись (dict).
        """
        import secrets

        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is empty")

        users = self._data.setdefault("users", {})

        if (user_id in users) and (not overwrite):
            raise ValueError(f"user_id already exists: {user_id}")

        if token is None:
            # короткий, но криптостойкий токен
            token = secrets.token_urlsafe(24)

        # дефолтные фильтры (вариант A: минимальные)
        default_filters = {
            "exclude_symbols": [],

            "volume_threshold": 20_000_000,
            "min_trades_24h": 10_000,
            "orderbook_min_bid": 20_000,
            "orderbook_min_ask": 20_000,

            "impulse": {
            "impulse_min_trades": 1000
            },

            "mark_delta": {"enabled": True, "pct": 1.0},
            "atr_impulse": {"enabled": True}
        }

        # если передали filters — смерджим поверх дефолтов
        final_filters = copy.deepcopy(default_filters)

        def deep_merge(dst: dict, src: dict):
            for k, v in src.items():
                if isinstance(v, dict) and isinstance(dst.get(k), dict):
                    deep_merge(dst[k], v)
                else:
                    dst[k] = v

        if isinstance(filters, dict):
            deep_merge(final_filters, filters)

        users[user_id] = {
            "token": token,
            "tg_chat_id": str(tg_chat_id) if tg_chat_id else None,
            "filters": final_filters,
            "created_at": _now(),
            "updated_at": _now(),
        }
        self.save()
        return users[user_id]



def _flatten(d: dict, prefix: str = "") -> dict:
    out = {}
    for k, v in (d or {}).items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(_flatten(v, key))
        else:
            out[key] = v
    return out

def _unflatten(flat: dict) -> dict:
    root = {}
    for k, v in (flat or {}).items():
        parts = str(k).split(".")
        cur = root
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = v
    return root

def _normalize_num(x):
    # приводим 20e6/строки/инты к float/инт корректно
    if isinstance(x, bool):
        return x
    try:
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return x
        if isinstance(x, str):
            if "." in x or "e" in x.lower():
                return float(x)
            return int(x)
    except Exception:
        pass
    return x

def _validate_patch(patch: dict) -> dict:
    """
    Разрешаем менять только ключи из ALLOWED_FILTERS и только на значения из списка.
    Остальное игнорируем.
    """
    flat = _flatten(patch)
    out = {}

    for k, v in flat.items():
        if k not in ALLOWED_FILTERS:
            continue

        v_norm = _normalize_num(v)

        allowed = ALLOWED_FILTERS[k]
        # сравнение чисел: лучше по float
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

    return _unflatten(out)

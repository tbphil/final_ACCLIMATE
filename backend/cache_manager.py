# cache_manager.py
from pathlib import Path
from cachetools import LRUCache
import gzip
import pickle
import hashlib
from typing import Any, Callable

class CacheManager:
    """
    Two-tier cache: an in-memory LRU (fast) backed by gz-compressed
    pickles on disk (shared across server restarts or multiple workers).

    Usage
    -----
    cache = CacheManager(Path("backend_cache"), mem_size=256)

    key = (lat, lon, scenario, ...)    # any hashable tuple
    result = cache.get("climate", key)
    if result is None:
        result = expensive_fn(...)
        cache.set("climate", key, result)
    """

    def __init__(self, dir_: Path, mem_size: int = 256):
        self.dir = dir_
        self.dir.mkdir(parents=True, exist_ok=True)
        self.mem = LRUCache(maxsize=mem_size)

    # ---------- helpers --------------------------------------------------
    @staticmethod
    def _hash_key(kind: str, key_tuple: tuple) -> str:
        """Stable SHA-256 hash of (kind, *key_tuple)."""
        raw = (kind, *key_tuple)
        return hashlib.sha256(repr(raw).encode()).hexdigest()

    def _fname(self, h: str) -> Path:
        return self.dir / f"{h}.pkl.gz"

    # ---------- public API ----------------------------------------------
    def get(self, kind: str, key_tuple: tuple):
        """
        Return cached object or None.

        Order of lookup:
        1. in-memory LRU
        2. gzip-pickle on disk
        """
        h = self._hash_key(kind, key_tuple)

        # 1) RAM hit
        if h in self.mem:
            return self.mem[h]

        # 2) Disk hit
        f = self._fname(h)
        if f.exists():
            obj = self._load(f)
            self.mem[h] = obj          # promote to RAM
            return obj

        # Miss
        return None

    def set(self, kind: str, key_tuple: tuple, obj):
        """Store object in both RAM (LRU) and disk (gz-pickle)."""
        h = self._hash_key(kind, key_tuple)
        self.mem[h] = obj
        self._dump(obj, self._fname(h))

    def clear(self):
        """Flush both tiers"""
        self.mem.clear()
        for f in self.dir.glob("*.pkl.gz"):
            f.unlink()

    # ---------- IO helpers ----------------------------------------------
    @staticmethod
    def _dump(obj, path: Path):
        with gzip.open(path, "wb") as fh:
            pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def _load(path: Path):
        with gzip.open(path, "rb") as fh:
            return pickle.load(fh)
        
cache = CacheManager(Path("backend_cache"), mem_size=256)    # 256 objects in-RAM, unlimited on disk

        
# ---------- cache helpers -------------------------------------------------
def _cache_or_run(section: str, key: tuple, builder: callable):
    """Try cache → build fresh → cache → return."""
    cached = cache.get(section, key)
    if cached is not None:
        return cached
    obj = builder()
    cache.set(section, key, obj)
    return obj


import contextlib
import json
import os
from pathlib import Path


def _empty_state():
    return {"prices": {}, "news": {}}


class CheckpointStore:
    def __init__(self, path):
        self.path = Path(path)

    def load(self):
        if not self.path.exists():
            return _empty_state()
        text = self.path.read_text(encoding="utf-8").strip()
        if not text:
            return _empty_state()
        state = json.loads(text)
        if not isinstance(state, dict):
            raise ValueError("Checkpoint root must be a mapping")
        for section in ("prices", "news"):
            state.setdefault(section, {})
            if not isinstance(state[section], dict):
                raise ValueError(
                    "Checkpoint section '%s' must be a mapping" % section
                )
        return state

    @contextlib.contextmanager
    def _exclusive(self):
        """Cross-process mutual exclusion for read-modify-write updates.

        collect-prices and collect-news can run as separate processes against
        the same checkpoint file; without a lock, one writer's load-modify-save
        can overwrite the other's update, regressing its checkpoint. A sidecar
        lock file serialises them (msvcrt on Windows, fcntl elsewhere; both are
        released by the OS if the holder dies).
        """
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(lock_path, "a+b")
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()

    def _save(self, state):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Per-process temp name: two writers sharing one .tmp raced each
        # other's os.replace into FileNotFoundError.
        temporary = self.path.with_suffix(
            "{0}.{1}.tmp".format(self.path.suffix, os.getpid())
        )
        temporary.write_text(
            json.dumps(state, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(str(temporary), str(self.path))

    def update_price(self, symbol, last_event_date):
        self._advance("prices", "last_event_date", symbol, last_event_date)

    def update_news(self, symbol, last_published_at_utc):
        self._advance("news", "last_published_at_utc", symbol, last_published_at_utc)

    def _advance(self, section, field, symbol, value):
        """Watermarks only move forward: a slower writer finishing after a
        faster one must not regress what is already durably recorded."""
        with self._exclusive():
            state = self.load()
            key = str(symbol).upper()
            value = str(value)
            current = state[section].get(key, {}).get(field)
            if current is not None and value <= current:
                return
            state[section][key] = {field: value}
            self._save(state)

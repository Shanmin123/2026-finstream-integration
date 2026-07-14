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
        state.setdefault("prices", {})
        state.setdefault("news", {})
        return state

    def _save(self, state):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(state, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(str(temporary), str(self.path))

    def update_price(self, symbol, last_event_date):
        state = self.load()
        state["prices"][str(symbol).upper()] = {"last_event_date": str(last_event_date)}
        self._save(state)

    def update_news(self, symbol, last_published_at_utc):
        state = self.load()
        state["news"][str(symbol).upper()] = {
            "last_published_at_utc": str(last_published_at_utc)
        }
        self._save(state)

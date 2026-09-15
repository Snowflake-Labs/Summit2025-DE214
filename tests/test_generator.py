import datetime

import pytest

import generator


class FailingSink:
    def __init__(self):
        self.closed = 0

    def append_batches(self, batches):
        raise RuntimeError("delivery failed")

    def close(self):
        self.closed += 1


def test_event_loop_propagates_delivery_failure(monkeypatch):
    sink = FailingSink()
    monkeypatch.setattr(generator, "SnowflakeStreamingSink", lambda: sink)
    if not hasattr(generator.datetime, "UTC"):
        monkeypatch.setattr(generator.datetime, "UTC", datetime.timezone.utc, raising=False)
    data_generator = generator.DataGenerator()

    with pytest.raises(RuntimeError, match="delivery failed"):
        data_generator.event_loop()

    assert sink.closed == 1

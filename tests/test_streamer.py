from concurrent.futures import Future

import pytest

import streamer


def completed_future(error=None):
    future = Future()
    if error is None:
        future.set_result(None)
    else:
        future.set_exception(error)
    return future


class FakeChannel:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def append_rows_with_wait(self, rows, token):
        self.calls.append((rows, token))
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class FakeIngestError(Exception):
    def __init__(self, status):
        self.http_status_code = status


def make_sink(channels):
    sink = object.__new__(streamer.SnowflakeStreamingSink)
    sink._clients = {}
    sink._channels = channels
    sink._append_seq = 0
    return sink


def test_waits_for_all_pipe_futures_before_raising():
    failed = completed_future(RuntimeError("failed"))
    succeeded = completed_future()
    sink = make_sink(
        {
            "resort_tickets": FakeChannel(failed),
            "season_passes": FakeChannel(succeeded),
        }
    )

    with pytest.raises(RuntimeError, match="resort_tickets"):
        sink.append_batches(
            {"resort_tickets": [{"id": 1}], "season_passes": [{"id": 2}]}
        )

    assert failed.done()
    assert succeeded.done()


def test_synchronous_append_failure_does_not_skip_sibling_future():
    succeeded = completed_future()
    sink = make_sink(
        {
            "resort_tickets": FakeChannel(RuntimeError("append failed")),
            "season_passes": FakeChannel(succeeded),
        }
    )

    with pytest.raises(RuntimeError, match="resort_tickets"):
        sink.append_batches(
            {"resort_tickets": [{"id": 1}], "season_passes": [{"id": 2}]}
        )

    assert succeeded.done()


def test_synchronous_backpressure_retries_the_same_batch(monkeypatch):
    rows = [{"id": 1}]
    future = completed_future()
    calls = []

    class BackpressuredChannel:
        def append_rows_with_wait(self, received_rows, token):
            calls.append(received_rows)
            if len(calls) == 1:
                raise FakeIngestError(429)
            return future

    monkeypatch.setattr(streamer, "StreamingIngestError", FakeIngestError)
    monkeypatch.setattr(streamer.time, "sleep", lambda seconds: None)

    returned = streamer._append_with_backpressure_wait(
        BackpressuredChannel(), rows, "batch-1"
    )

    assert returned is future
    assert calls == [rows, rows]

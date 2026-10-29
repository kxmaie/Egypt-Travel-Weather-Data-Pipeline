import pytest

from include.ingestion import landmarks_api, weather_api


class FakeMsSqlHook:
    def __init__(self, **kwargs):
        self.run_calls = []

    def run(self, sql, parameters=None):
        self.run_calls.append((sql, parameters))

    def get_records(self, sql, parameters=None):
        assert isinstance(parameters, tuple)
        return [(0,)]


class FailedResponse:
    text = "{}"

    def raise_for_status(self):
        raise RuntimeError("upstream API failed")


@pytest.mark.parametrize(
    ("module", "callable_name"),
    [
        (weather_api, "data_of_weather_api"),
        (landmarks_api, "famous_landmark_data"),
    ],
)
def test_api_failure_fails_task_instead_of_publishing_stale_success(
    monkeypatch, module, callable_name
):
    hooks = []

    def make_hook(**kwargs):
        hook = FakeMsSqlHook(**kwargs)
        hooks.append(hook)
        return hook

    monkeypatch.setattr(module, "MsSqlHook", make_hook)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: FailedResponse())
    if module is landmarks_api:
        monkeypatch.setattr(module.Variable, "get", lambda *args, **kwargs: "test-key")

    with pytest.raises(RuntimeError, match="upstream API failed"):
        getattr(module, callable_name)()

    assert hooks
    # The only SQL call is CREATE TABLE; failed API data is never inserted.
    assert len(hooks[0].run_calls) == 1


def test_api_requests_set_connect_and_read_timeouts(monkeypatch):
    calls = []
    hook = FakeMsSqlHook()
    monkeypatch.setattr(weather_api, "MsSqlHook", lambda **kwargs: hook)

    def fail_after_recording(url, **kwargs):
        calls.append(kwargs)
        return FailedResponse()

    monkeypatch.setattr(weather_api.requests, "get", fail_after_recording)
    with pytest.raises(RuntimeError, match="upstream API failed"):
        weather_api.data_of_weather_api()

    assert calls[0]["timeout"] == (5, 30)

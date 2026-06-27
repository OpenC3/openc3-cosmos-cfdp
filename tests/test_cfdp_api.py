import json
from unittest.mock import MagicMock

import pytest
from cfdp_api import CfdpApi


class FakeResponse:
    def __init__(self, status_code=200, text=""):
        self.status_code = status_code
        self.text = text


def make_api(response):
    """Build a CfdpApi with a stubbed _request that returns `response` and
    records the call args on api.last_call. Bypass __init__ so no network /
    authentication happens during construction."""
    api = CfdpApi.__new__(CfdpApi)
    api.last_call = {}

    def fake_request(method, endpoint, **kwargs):
        api.last_call = {"method": method, "endpoint": endpoint, **kwargs}
        return response

    api._request = MagicMock(side_effect=fake_request)
    return api


def test_put_builds_data_and_returns_body():
    api = make_api(FakeResponse(200, "12345"))
    result = api.put(
        destination_entity_id="2",
        source_file_name="a.bin",
        destination_file_name="b.bin",
        transmission_mode="ACKNOWLEDGED",
    )
    assert result == "12345"
    assert api.last_call["method"] == "post"
    assert api.last_call["endpoint"] == "/put"
    data = api.last_call["data"]
    assert data["destination_entity_id"] == 2  # converted to int
    assert data["source_file_name"] == "a.bin"
    assert data["destination_file_name"] == "b.bin"
    assert data["transmission_mode"] == "ACKNOWLEDGED"
    assert data["filestore_requests"] == []
    assert data["segmentation_control"] == "NOT_PRESERVED"
    assert "remote_entity_id" not in data


def test_put_includes_remote_entity_id_as_int():
    api = make_api(FakeResponse(200, "1"))
    api.put(
        destination_entity_id=2,
        source_file_name="a.bin",
        destination_file_name="b.bin",
        remote_entity_id="5",
    )
    assert api.last_call["data"]["remote_entity_id"] == 5


def test_put_error_status_raises():
    api = make_api(FakeResponse(500, "boom"))
    with pytest.raises(RuntimeError) as exc:
        api.put(destination_entity_id=2, source_file_name="a", destination_file_name="b")
    assert "put failed due to" in str(exc.value)
    assert "500" in str(exc.value)


def test_put_none_response_raises():
    api = make_api(None)
    with pytest.raises(RuntimeError) as exc:
        api.put(destination_entity_id=2, source_file_name="a", destination_file_name="b")
    assert "put failed" in str(exc.value)


def test_put_dir_parses_json():
    api = make_api(FakeResponse(200, json.dumps(["a", "b"])))
    result = api.put_dir(destination_entity_id=2, source_directory_name="/dir")
    assert result == ["a", "b"]
    assert api.last_call["endpoint"] == "/put_dir"
    assert api.last_call["data"]["source_directory_name"] == "/dir"


def test_subscribe_returns_body():
    api = make_api(FakeResponse(200, "5-0"))
    assert api.subscribe() == "5-0"
    assert api.last_call["method"] == "get"
    assert api.last_call["endpoint"] == "/subscribe"


def test_indications_builds_endpoint_and_query():
    api = make_api(FakeResponse(200, json.dumps({"continuation": "1-0", "indications": []})))
    result = api.indications(transaction_id="abc", continuation="0-0", limit=50)
    assert result == {"continuation": "1-0", "indications": []}
    assert api.last_call["endpoint"] == "/indications/abc"
    assert api.last_call["query"] == {"continuation": "0-0", "limit": 50}


def test_indications_no_transaction_id():
    api = make_api(FakeResponse(200, json.dumps({"continuation": "1-0", "indications": []})))
    api.indications()
    assert api.last_call["endpoint"] == "/indications"
    # continuation omitted when falsy, limit defaults to 100
    assert api.last_call["query"] == {"limit": 100}


def test_transactions_parses_json_and_query():
    api = make_api(FakeResponse(200, json.dumps([{"id": "1"}])))
    result = api.transactions(active=True)
    assert result == [{"id": "1"}]
    assert api.last_call["endpoint"] == "/transactions"
    assert api.last_call["query"] == {"active": True}


def test_transactions_inactive_omits_query():
    api = make_api(FakeResponse(200, json.dumps([])))
    api.transactions(active=False)
    assert api.last_call["query"] == {}


def test_directory_listing_posts_data():
    api = make_api(FakeResponse(200, "ok"))
    result = api.directory_listing(
        remote_entity_id=2, directory_name="/d", directory_file_name="/d.txt"
    )
    assert result == "ok"
    assert api.last_call["endpoint"] == "/directorylisting"
    assert api.last_call["data"] == {
        "remote_entity_id": 2,
        "directory_name": "/d",
        "directory_file_name": "/d.txt",
    }


def test_transaction_id_post_null_returns_none():
    api = make_api(FakeResponse(200, "null"))
    assert api.cancel(transaction_id="t1") is None
    assert api.last_call["endpoint"] == "/cancel"
    assert api.last_call["data"]["transaction_id"] == "t1"


def test_transaction_id_post_returns_body():
    api = make_api(FakeResponse(200, "t1"))
    assert api.suspend(transaction_id="t1") == "t1"
    assert api.last_call["endpoint"] == "/suspend"


def test_report_includes_optional_fields():
    api = make_api(FakeResponse(200, "t1"))
    api.report(transaction_id="t1", remote_entity_id=2, report_file_name="r.txt")
    data = api.last_call["data"]
    assert data["transaction_id"] == "t1"
    assert data["remote_entity_id"] == 2
    assert data["report_file_name"] == "r.txt"


def test_transaction_id_post_error_raises():
    api = make_api(FakeResponse(404, "nope"))
    with pytest.raises(RuntimeError) as exc:
        api.resume(transaction_id="t1")
    assert "resume failed due to" in str(exc.value)
    assert "404" in str(exc.value)

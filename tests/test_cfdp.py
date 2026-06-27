from unittest.mock import MagicMock, patch

import cfdp
import pytest


def make_indications(*indications, continuation="1-0"):
    return {"continuation": continuation, "indications": list(indications)}


# ----- cfdp_wait_for_indication -----


def test_wait_returns_matching_indication():
    api = MagicMock()
    api.indications.return_value = make_indications(
        {"indication_type": "Other"},
        {"indication_type": "Transaction-Finished"},
    )
    result = cfdp.cfdp_wait_for_indication(
        api=api, indication_type="Transaction-Finished", timeout=10
    )
    assert result == {"indication_type": "Transaction-Finished"}


def test_wait_include_continuation_returns_tuple():
    api = MagicMock()
    api.indications.return_value = make_indications(
        {"indication_type": "Report"}, continuation="9-0"
    )
    result, continuation = cfdp.cfdp_wait_for_indication(
        api=api, indication_type="Report", include_continuation=True, timeout=10
    )
    assert result == {"indication_type": "Report"}
    assert continuation == "9-0"


def test_wait_no_indication_type_returns_all():
    api = MagicMock()
    indications = [{"indication_type": "A"}, {"indication_type": "B"}]
    api.indications.return_value = make_indications(*indications)
    result = cfdp.cfdp_wait_for_indication(api=api, timeout=10)
    assert result == indications


def test_wait_timeout_zero_no_match_returns_none():
    api = MagicMock()
    api.indications.return_value = make_indications()  # empty
    result = cfdp.cfdp_wait_for_indication(api=api, indication_type="Nope", timeout=None)
    assert result is None


def test_wait_timeout_zero_no_type_returns_empty_list():
    api = MagicMock()
    api.indications.return_value = make_indications()
    result = cfdp.cfdp_wait_for_indication(api=api, timeout=None)
    assert result == []


def test_wait_raises_on_timeout():
    api = MagicMock()
    api.indications.return_value = make_indications()  # never matches
    with patch.object(cfdp.time, "time", side_effect=[1000.0, 1001.0]):
        with pytest.raises(RuntimeError, match="CFDP Timeout"):
            cfdp.cfdp_wait_for_indication(api=api, indication_type="Nope", timeout=0.5)


# ----- wrapper functions -----


def test_cfdp_put_returns_id_and_indication():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.put.return_value = "tid-1"
    api.indications.return_value = make_indications({"indication_type": "Transaction-Finished"})
    with patch.object(cfdp, "CfdpApi", return_value=api):
        transaction_id, indication = cfdp.cfdp_put(
            destination_entity_id=2, source_file_name="a", destination_file_name="b"
        )
    assert transaction_id == "tid-1"
    assert indication == {"indication_type": "Transaction-Finished"}


def test_cfdp_put_no_timeout_returns_id_only():
    api = MagicMock()
    api.put.return_value = "tid-1"
    with patch.object(cfdp, "CfdpApi", return_value=api):
        result = cfdp.cfdp_put(
            destination_entity_id=2, source_file_name="a", destination_file_name="b", timeout=None
        )
    assert result == "tid-1"
    api.subscribe.assert_not_called()


def test_cfdp_put_proxy_waits_for_proxy_put_response():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.put.return_value = "tid-1"
    api.indications.return_value = make_indications({"indication_type": "Proxy-Put-Response"})
    with patch.object(cfdp, "CfdpApi", return_value=api):
        _, indication = cfdp.cfdp_put(
            destination_entity_id=1,
            remote_entity_id=2,
            source_file_name="a",
            destination_file_name="b",
        )
    assert indication == {"indication_type": "Proxy-Put-Response"}


def test_cfdp_cancel_returns_indication():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.cancel.return_value = "tid-1"
    api.indications.return_value = make_indications({"indication_type": "Transaction-Finished"})
    with patch.object(cfdp, "CfdpApi", return_value=api):
        indication = cfdp.cfdp_cancel(transaction_id="tid-1")
    assert indication == {"indication_type": "Transaction-Finished"}


def test_cfdp_cancel_no_transaction_returns_none():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.cancel.return_value = None
    with patch.object(cfdp, "CfdpApi", return_value=api):
        assert cfdp.cfdp_cancel(transaction_id="tid-1") is None


def test_cfdp_subscribe_delegates():
    api = MagicMock()
    api.subscribe.return_value = "7-0"
    with patch.object(cfdp, "CfdpApi", return_value=api):
        assert cfdp.cfdp_subscribe() == "7-0"


def test_cfdp_transactions_delegates():
    api = MagicMock()
    api.transactions.return_value = [{"id": "1"}]
    with patch.object(cfdp, "CfdpApi", return_value=api):
        assert cfdp.cfdp_transactions() == [{"id": "1"}]


def test_cfdp_put_dir_returns_ids_and_indications():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.put_dir.return_value = ["t1", "t2"]
    api.indications.return_value = make_indications({"indication_type": "Transaction-Finished"})
    with patch.object(cfdp, "CfdpApi", return_value=api):
        ids, indications = cfdp.cfdp_put_dir(destination_entity_id=2, source_directory_name="/d")
    assert ids == ["t1", "t2"]
    assert len(indications) == 2
    assert indications[0] == ["t1", {"indication_type": "Transaction-Finished"}]


def test_cfdp_directory_listing_returns_indications():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.directory_listing.return_value = "tid-1"
    api.indications.side_effect = [
        make_indications({"indication_type": "Transaction-Finished"}),
        make_indications({"indication_type": "Directory-Listing-Response"}),
    ]
    with patch.object(cfdp, "CfdpApi", return_value=api):
        result = cfdp.cfdp_directory_listing(
            remote_entity_id=2, directory_name="/d", directory_file_name="/d.txt", timeout=10
        )
    assert result == [
        {"indication_type": "Transaction-Finished"},
        {"indication_type": "Directory-Listing-Response"},
    ]


def test_cfdp_directory_listing_no_transaction_returns_empty():
    api = MagicMock()
    api.subscribe.return_value = "0-0"
    api.directory_listing.return_value = None
    with patch.object(cfdp, "CfdpApi", return_value=api):
        result = cfdp.cfdp_directory_listing(
            remote_entity_id=2, directory_name="/d", directory_file_name="/d.txt", timeout=10
        )
    assert result == []

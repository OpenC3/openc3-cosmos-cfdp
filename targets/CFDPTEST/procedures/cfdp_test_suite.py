import io

from cfdp import (
    cfdp_cancel,
    cfdp_indications,
    cfdp_put,
    cfdp_report,
    cfdp_resume,
    cfdp_subscribe,
    cfdp_suspend,
    cfdp_transactions,
)
from openc3.script import (
    delete_target_file,
    put_target_file,
    set_line_delay,
    wait,
)
from openc3.script.suite import Group, Suite


# Group class name should indicate what the scripts are testing
class CfdpTestGroup(Group):
    def test_standard_ops(self):
        set_line_delay(0)

        # Simple put and wait for complete
        cfdp_put(
            destination_entity_id=2,
            source_file_name="small.bin",
            destination_file_name="small2.bin",
            transmission_mode="ACKNOWLEDGED",
        )

        # Start and cancel
        transaction_id = cfdp_put(
            destination_entity_id=2,
            source_file_name="medium.bin",
            destination_file_name="medium2.bin",
            transmission_mode="ACKNOWLEDGED",
            timeout=None,
        )
        wait(1)
        cfdp_cancel(transaction_id=transaction_id)

        # Longer put and interact while running
        continuation = cfdp_subscribe()
        transaction_id = cfdp_put(
            destination_entity_id=2,
            source_file_name="medium.bin",
            destination_file_name="medium2.bin",
            transmission_mode="ACKNOWLEDGED",
            timeout=None,
        )
        wait(1)
        print(cfdp_suspend(transaction_id=transaction_id))
        wait(1)
        print(cfdp_report(transaction_id=transaction_id))
        wait(1)
        print(cfdp_transactions())
        wait(1)
        print(cfdp_resume(transaction_id=transaction_id))
        # Watch the transaction progress
        set_line_delay(0.1)
        done = False
        while True:
            indications, continuation = cfdp_indications(continuation=continuation)
            for indication in indications:
                print(repr(indication))
                if indication["indication_type"] == "Transaction-Finished":
                    done = True
                    break
            # print(cfdp_report(transaction_id=transaction_id))
            print(repr(cfdp_transactions()))
            if done:
                break

    def test_proxy_ops(self):
        set_line_delay(0)

        # Simple proxy put and wait for complete
        cfdp_put(
            remote_entity_id=2,
            destination_entity_id=1,
            source_file_name="small.bin",
            destination_file_name="small2.bin",
            transmission_mode="ACKNOWLEDGED",
        )

        # Start and cancel
        transaction_id = cfdp_put(
            remote_entity_id=2,
            destination_entity_id=1,
            source_file_name="medium.bin",
            destination_file_name="medium2.bin",
            transmission_mode="ACKNOWLEDGED",
            timeout=None,
        )
        wait(1)
        cfdp_cancel(remote_entity_id=2, transaction_id=transaction_id)

        print(cfdp_transactions(microservice_name="CFDP2", prefix="/cfdp2", port=2906))

        # Longer proxy put and interact while running
        continuation = cfdp_subscribe()
        source_transaction_id = cfdp_put(
            remote_entity_id=2,
            destination_entity_id=1,
            source_file_name="medium.bin",
            destination_file_name="medium2.bin",
            transmission_mode="ACKNOWLEDGED",
            timeout=None,
        )
        wait(1)

        # Get the most recent transaction id from the other entity and assume
        # that is the right one... The CFDP spec is lacking here
        transactions = cfdp_transactions(microservice_name="CFDP2", prefix="/cfdp2", port=2906)
        transaction_id = transactions[-1]["id"]
        print(cfdp_suspend(remote_entity_id=2, transaction_id=transaction_id))
        wait(1)
        print(
            cfdp_report(
                remote_entity_id=2, transaction_id=transaction_id, report_file_name="myreport.txt"
            )
        )
        wait(1)
        print(cfdp_transactions(microservice_name="CFDP2", prefix="/cfdp2", port=2906))
        wait(1)
        print(cfdp_resume(remote_entity_id=2, transaction_id=transaction_id))
        # Watch the transaction progress
        set_line_delay(0)
        done = False
        while True:
            indications, continuation = cfdp_indications(continuation=continuation)
            for indication in indications:
                if indication["indication_type"] != "File-Segment-Recv":
                    print(repr(indication))
                if (
                    indication["indication_type"] == "Proxy-Put-Response"
                    and indication["transaction_id"] == source_transaction_id
                ):
                    done = True
                    break
            wait(0.1)
            # print(cfdp_report(transaction_id=transaction_id))
            print(cfdp_transactions(microservice_name="CFDP2", prefix="/cfdp2", port=2906))
            if done:
                break

    def setup(self):
        set_line_delay(0)

        # Create test files
        data = b"\x00" * 1000

        # small.bin
        file = io.BytesIO()
        for _ in range(500):
            file.write(data)
        file.seek(0)
        put_target_file("/CFDP/tmp/small.bin", file)

        # medium.bin
        file = io.BytesIO()
        for _ in range(5000):
            file.write(data)
        file.seek(0)
        put_target_file("/CFDP/tmp/medium.bin", file)

    def teardown(self):
        delete_target_file("/CFDP/tmp/small.bin")
        delete_target_file("/CFDP/tmp/medium.bin")


class CfdpTestSuite(Suite):
    def __init__(self):
        super().__init__()
        self.add_group(CfdpTestGroup)

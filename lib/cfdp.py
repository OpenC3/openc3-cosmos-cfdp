# Copyright 2026 OpenC3, Inc.
# All Rights Reserved.
#
# Licensed for Evaluation and Educational Use
#
# This file may only be used commercially under the terms of a commercial license
# purchased from OpenC3, Inc.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# The development of this software was funded in-whole or in-part by MethaneSAT LLC.

import time

from cfdp_api import CfdpApi
from openc3.environment import OPENC3_SCOPE


def _get_wait():
    # Available when running inside ScriptRunner. Returns the actual time waited;
    # a value less than the requested time means the user hit Go. Imported lazily
    # (mirrors the Ruby `defined? wait`) so simply importing cfdp does not pull in
    # the full openc3.script stack.
    try:
        from openc3.script import wait

        return wait
    except Exception:
        return None


def cfdp_put(
    destination_entity_id,
    source_file_name=None,
    destination_file_name=None,
    closure_requested=None,
    transmission_mode=None,
    filestore_requests=None,
    fault_handler_overrides=None,
    flow_label=None,
    segmentation_control="NOT_PRESERVED",
    messages_to_user=None,
    remote_entity_id=None,  # Used to indicate proxy put
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_id = api.put(
        destination_entity_id=destination_entity_id,
        source_file_name=source_file_name,
        destination_file_name=destination_file_name,
        transmission_mode=transmission_mode,
        closure_requested=closure_requested,
        filestore_requests=filestore_requests,
        fault_handler_overrides=fault_handler_overrides,
        flow_label=flow_label,
        segmentation_control=segmentation_control,
        messages_to_user=messages_to_user,
        remote_entity_id=remote_entity_id,  # Used to indicate proxy put
        scope=scope,
    )
    if not timeout:
        return transaction_id
    indication_type = "Proxy-Put-Response" if remote_entity_id else "Transaction-Finished"
    indication = cfdp_wait_for_indication(
        api=api,
        transaction_id=transaction_id,
        indication_type=indication_type,
        continuation=continuation,
        timeout=timeout,
        scope=scope,
    )
    return transaction_id, indication


def cfdp_put_dir(
    destination_entity_id,
    source_directory_name,
    closure_requested=None,
    transmission_mode=None,
    filestore_requests=None,
    fault_handler_overrides=None,
    flow_label=None,
    segmentation_control="NOT_PRESERVED",
    messages_to_user=None,
    remote_entity_id=None,  # Used to indicate proxy put
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_ids = api.put_dir(
        destination_entity_id=destination_entity_id,
        source_directory_name=source_directory_name,
        transmission_mode=transmission_mode,
        closure_requested=closure_requested,
        filestore_requests=filestore_requests,
        fault_handler_overrides=fault_handler_overrides,
        flow_label=flow_label,
        segmentation_control=segmentation_control,
        messages_to_user=messages_to_user,
        remote_entity_id=remote_entity_id,  # Used to indicate proxy put
        scope=scope,
    )
    if not timeout:
        return transaction_ids
    indications = []
    for transaction_id in transaction_ids:
        indication_type = "Proxy-Put-Response" if remote_entity_id else "Transaction-Finished"
        indication = cfdp_wait_for_indication(
            api=api,
            transaction_id=transaction_id,
            indication_type=indication_type,
            continuation=continuation,
            timeout=timeout,
            scope=scope,
        )
        indications.append([transaction_id, indication])
    return transaction_ids, indications


def cfdp_cancel(
    transaction_id,
    remote_entity_id=None,
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_id = api.cancel(
        transaction_id=transaction_id, remote_entity_id=remote_entity_id, scope=scope
    )
    indication = None
    if transaction_id:
        if timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Transaction-Finished",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
        if indication is None and remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Proxy-Put-Response",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
    return indication


def cfdp_suspend(
    transaction_id,
    remote_entity_id=None,
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_id = api.suspend(
        transaction_id=transaction_id, remote_entity_id=remote_entity_id, scope=scope
    )
    indication = None
    if transaction_id:
        if not remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Suspended",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
        if indication is None and remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Remote-Suspend-Response",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
    return indication


def cfdp_resume(
    transaction_id,
    remote_entity_id=None,
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_id = api.resume(
        transaction_id=transaction_id, remote_entity_id=remote_entity_id, scope=scope
    )
    indication = None
    if transaction_id:
        if not remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Resumed",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
        if indication is None and remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Remote-Resume-Response",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
    return indication


def cfdp_report(
    transaction_id,
    remote_entity_id=None,
    report_file_name=None,
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_id = api.report(
        transaction_id=transaction_id,
        remote_entity_id=remote_entity_id,
        report_file_name=report_file_name,
        scope=scope,
    )
    indication = None
    if transaction_id:
        if not remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Report",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
        if indication is None and remote_entity_id and timeout:
            indication = cfdp_wait_for_indication(
                api=api,
                transaction_id=transaction_id,
                indication_type="Remote-Report-Response",
                continuation=continuation,
                timeout=timeout,
                scope=scope,
            )
    return indication


def cfdp_subscribe(
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    return api.subscribe(scope=scope)


def cfdp_indications(
    transaction_id=None,
    indication_type=None,
    continuation=None,
    include_continuation=True,
    timeout=None,
    api_timeout=5,
    limit=100,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    return cfdp_wait_for_indication(
        api=api,
        transaction_id=transaction_id,
        indication_type=indication_type,
        continuation=continuation,
        include_continuation=include_continuation,
        timeout=timeout,
        limit=limit,
        scope=scope,
    )


def cfdp_directory_listing(
    remote_entity_id,
    directory_name,
    directory_file_name,
    timeout=600,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    continuation = api.subscribe(scope=scope) if timeout else None
    transaction_id = api.directory_listing(
        remote_entity_id=remote_entity_id,
        directory_name=directory_name,
        directory_file_name=directory_file_name,
        scope=scope,
    )
    indications = []
    if transaction_id:
        if timeout:
            indications.append(
                cfdp_wait_for_indication(
                    api=api,
                    transaction_id=transaction_id,
                    indication_type="Transaction-Finished",
                    continuation=continuation,
                    timeout=timeout,
                    scope=scope,
                )
            )
        if timeout:
            indications.append(
                cfdp_wait_for_indication(
                    api=api,
                    transaction_id=transaction_id,
                    indication_type="Directory-Listing-Response",
                    continuation=continuation,
                    timeout=timeout,
                    scope=scope,
                )
            )
    return [indication for indication in indications if indication is not None]


def cfdp_transactions(
    active=True,
    api_timeout=5,
    microservice_name="CFDP",
    prefix="/cfdp",
    schema="http",
    hostname=None,
    port=2905,
    url=None,
    scope=OPENC3_SCOPE,
):
    api = CfdpApi(
        timeout=api_timeout,
        microservice_name=microservice_name,
        prefix=prefix,
        schema=schema,
        hostname=hostname,
        port=port,
        url=url,
        scope=scope,
    )
    return api.transactions(active=active, scope=scope)


# Helper methods


def cfdp_wait_for_indication(
    api,
    transaction_id=None,
    indication_type=None,
    continuation=None,
    include_continuation=False,
    timeout=600,
    limit=1000,
    scope=OPENC3_SCOPE,
):
    if timeout is None:
        timeout = 0
    start_time = time.time()
    end_time = start_time + timeout
    if not continuation:
        continuation = "0-0"
    done = False
    while not done:
        result = api.indications(
            transaction_id=transaction_id, continuation=continuation, limit=limit, scope=scope
        )
        continuation = result["continuation"]
        indications = result["indications"]
        if indications and len(indications) > 0:
            if not indication_type:
                if include_continuation:
                    return indications, continuation
                else:
                    return indications
            for indication in indications:
                if indication["indication_type"] == indication_type:
                    if include_continuation:
                        return indication, continuation
                    else:
                        return indication
        if time.time() >= end_time:
            if timeout > 0:
                raise RuntimeError("CFDP Timeout")
            done = True
        if done:
            break
        wait_func = _get_wait()
        if wait_func is not None:
            wait_time = wait_func(1)
            if wait_time < 1.0:  # User hit go
                break
        else:
            time.sleep(1)
    if indication_type:
        if include_continuation:
            return None, continuation
        else:
            return None
    else:
        if include_continuation:
            return [], continuation
        else:
            return []

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

import json

from openc3.environment import OPENC3_SCOPE
from openc3.io.json_api import JsonApi

# Usage:
#
# Note: Recommend using the methods in cfdp.py rather than this file directly
#
# In ScriptRunner:
# from cfdp_api import CfdpApi
# api = CfdpApi()
# api.put(...)
#
# Outside cluster - Core
# os.environ['OPENC3_SCOPE'] = 'DEFAULT'
# os.environ['OPENC3_API_PASSWORD'] = 'password'
# from cfdp_api import CfdpApi
# api = CfdpApi(url="http://127.0.0.1:2900/cfdp")
# api.put(...)
#
# Outside cluster - Enterprise
# os.environ['OPENC3_SCOPE'] = 'DEFAULT'
# os.environ['OPENC3_KEYCLOAK_URL'] = 'http://127.0.0.1:2900/auth'
# os.environ['OPENC3_API_USER'] = 'operator'
# os.environ['OPENC3_API_PASSWORD'] = 'operator'
# from cfdp_api import CfdpApi
# api = CfdpApi(url="http://127.0.0.1:2900/cfdp")
# api.put(...)
#


class CfdpApi(JsonApi):
    def __init__(
        self,
        microservice_name="CFDP",
        prefix="/cfdp",
        port=2905,
        schema="http",
        hostname=None,
        timeout=5.0,
        url=None,
        scope=OPENC3_SCOPE,
    ):
        super().__init__(
            microservice_name=microservice_name,
            prefix=prefix,
            port=port,
            schema=schema,
            hostname=hostname,
            timeout=timeout,
            url=url,
            scope=scope,
        )

    def put(
        self,
        destination_entity_id,
        source_file_name,
        destination_file_name,
        transmission_mode=None,
        closure_requested=None,
        filestore_requests=None,
        fault_handler_overrides=None,
        flow_label=None,
        segmentation_control="NOT_PRESERVED",
        messages_to_user=None,
        remote_entity_id=None,  # Used to indicate proxy put
        scope=OPENC3_SCOPE,
    ):
        try:
            endpoint = "/put"
            data = {
                "destination_entity_id": int(destination_entity_id),
                "source_file_name": source_file_name,
                "destination_file_name": destination_file_name,
                "transmission_mode": transmission_mode,
                "closure_requested": closure_requested,
                "filestore_requests": filestore_requests if filestore_requests is not None else [],
                "fault_handler_overrides": fault_handler_overrides
                if fault_handler_overrides is not None
                else [],
                "messages_to_user": messages_to_user if messages_to_user is not None else [],
                "flow_label": flow_label,
                "segmentation_control": segmentation_control,
            }
            if remote_entity_id is not None:
                data["remote_entity_id"] = int(remote_entity_id)
            response = self._request("post", endpoint, data=data, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(f"CFDP put error: {response.status_code}: {response.text}")
                else:
                    raise RuntimeError("CFDP put failed")
            return response.text
        except Exception as error:
            raise RuntimeError(f"CFDP put failed due to {repr(error)}") from error

    def put_dir(
        self,
        destination_entity_id,
        source_directory_name,
        transmission_mode=None,
        closure_requested=None,
        filestore_requests=None,
        fault_handler_overrides=None,
        flow_label=None,
        segmentation_control="NOT_PRESERVED",
        messages_to_user=None,
        remote_entity_id=None,  # Used to indicate proxy put
        scope=OPENC3_SCOPE,
    ):
        try:
            endpoint = "/put_dir"
            data = {
                "destination_entity_id": int(destination_entity_id),
                "source_directory_name": source_directory_name,
                "transmission_mode": transmission_mode,
                "closure_requested": closure_requested,
                "filestore_requests": filestore_requests if filestore_requests is not None else [],
                "fault_handler_overrides": fault_handler_overrides
                if fault_handler_overrides is not None
                else [],
                "messages_to_user": messages_to_user if messages_to_user is not None else [],
                "flow_label": flow_label,
                "segmentation_control": segmentation_control,
            }
            if remote_entity_id is not None:
                data["remote_entity_id"] = int(remote_entity_id)
            response = self._request("post", endpoint, data=data, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(
                        f"CFDP put_dir error: {response.status_code}: {response.text}"
                    )
                else:
                    raise RuntimeError("CFDP put_dir failed")
            return json.loads(response.text)
        except Exception as error:
            raise RuntimeError(f"CFDP put_dir failed due to {repr(error)}") from error

    def cancel(self, transaction_id, remote_entity_id=None, scope=OPENC3_SCOPE):
        return self.transaction_id_post(
            method_name="cancel",
            transaction_id=transaction_id,
            remote_entity_id=remote_entity_id,
            scope=scope,
        )

    def suspend(self, transaction_id, remote_entity_id=None, scope=OPENC3_SCOPE):
        return self.transaction_id_post(
            method_name="suspend",
            transaction_id=transaction_id,
            remote_entity_id=remote_entity_id,
            scope=scope,
        )

    def resume(self, transaction_id, remote_entity_id=None, scope=OPENC3_SCOPE):
        return self.transaction_id_post(
            method_name="resume",
            transaction_id=transaction_id,
            remote_entity_id=remote_entity_id,
            scope=scope,
        )

    def report(
        self, transaction_id, remote_entity_id=None, report_file_name=None, scope=OPENC3_SCOPE
    ):
        return self.transaction_id_post(
            method_name="report",
            transaction_id=transaction_id,
            remote_entity_id=remote_entity_id,
            report_file_name=report_file_name,
            scope=scope,
        )

    def subscribe(self, scope=OPENC3_SCOPE):
        try:
            endpoint = "/subscribe"
            response = self._request("get", endpoint, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(
                        f"CFDP subscribe error: {response.status_code}: {response.text}"
                    )
                else:
                    raise RuntimeError("CFDP subscribe failed")
            # Most recent topic id
            return response.text
        except Exception as error:
            raise RuntimeError(f"CFDP subscribe failed due to {repr(error)}") from error

    def indications(self, transaction_id=None, continuation=None, limit=100, scope=OPENC3_SCOPE):
        try:
            endpoint = "/indications"
            if transaction_id:
                endpoint += "/" + str(transaction_id)
            query = {}
            if continuation:
                query["continuation"] = continuation
            if limit:
                query["limit"] = limit
            response = self._request("get", endpoint, query=query, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(
                        f"CFDP indications error: {response.status_code}: {response.text}"
                    )
                else:
                    raise RuntimeError("CFDP indications failed")
            # Dict of continuation, and indications array
            return json.loads(response.text)
        except Exception as error:
            raise RuntimeError(f"CFDP indications failed due to {repr(error)}") from error

    def directory_listing(
        self, remote_entity_id, directory_name, directory_file_name, scope=OPENC3_SCOPE
    ):
        try:
            endpoint = "/directorylisting"
            data = {
                "remote_entity_id": remote_entity_id,
                "directory_name": directory_name,
                "directory_file_name": directory_file_name,
            }
            response = self._request("post", endpoint, data=data, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(
                        f"CFDP directory listing error: {response.status_code}: {response.text}"
                    )
                else:
                    raise RuntimeError("CFDP  directory listing failed")
            return response.text
        except Exception as error:
            raise RuntimeError(f"CFDP  directory listing failed due to {repr(error)}") from error

    def transactions(self, active=True, scope=OPENC3_SCOPE):
        try:
            endpoint = "/transactions"
            query = {}
            if active:
                query["active"] = active
            response = self._request("get", endpoint, query=query, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(
                        f"CFDP transactions error: {response.status_code}: {response.text}"
                    )
                else:
                    raise RuntimeError("CFDP transactions failed")
            # Array of Transaction dicts
            return json.loads(response.text)
        except Exception as error:
            raise RuntimeError(f"CFDP transactions failed due to {repr(error)}") from error

    # private

    def transaction_id_post(
        self,
        method_name,
        transaction_id,
        remote_entity_id=None,
        report_file_name=None,
        scope=OPENC3_SCOPE,
    ):
        try:
            endpoint = f"/{method_name}"
            data = {"transaction_id": str(transaction_id)}
            if remote_entity_id is not None:
                data["remote_entity_id"] = remote_entity_id
            if report_file_name is not None:
                data["report_file_name"] = report_file_name
            response = self._request("post", endpoint, data=data, scope=scope)
            if response is None or response.status_code != 200:
                if response is not None:
                    raise RuntimeError(
                        f"CFDP {method_name} error: {response.status_code}: {response.text}"
                    )
                else:
                    raise RuntimeError(f"CFDP {method_name} failed")
            # String of "null" results from the controller "render json: null"
            if response.text == "null":
                return None
            else:
                return response.text
        except Exception as error:
            raise RuntimeError(f"CFDP {method_name} failed due to {repr(error)}") from error

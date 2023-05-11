# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
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

require_relative 'cfdp_api'

def cfdp_put(
  destination_entity_id:,
  source_file_name: nil,
  destination_file_name: nil,
  closure_requested: nil,
  transmission_mode: nil,
  filestore_requests: [],
  fault_handler_overrides: [],
  flow_label: nil,
  segmentation_control: "NOT_PRESERVED",
  messages_to_user: [],
  remote_entity_id: nil, # Used to indicate proxy put
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)

  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.put(
    destination_entity_id: destination_entity_id,
    source_file_name: source_file_name,
    destination_file_name: destination_file_name,
    transmission_mode: transmission_mode,
    closure_requested: closure_requested,
    filestore_requests: filestore_requests,
    fault_handler_overrides: fault_handler_overrides,
    flow_label: flow_label,
    segmentation_control: segmentation_control,
    messages_to_user: messages_to_user,
    remote_entity_id: remote_entity_id, # Used to indicate proxy put
    scope: $openc3_scope)
  return transaction_id unless timeout
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Transaction-Finished', timeout: timeout) if timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Proxy-Put-Response', timeout: timeout) if remote_entity_id and timeout
  return transaction_id, indication
end

def cfdp_cancel(
  transaction_id:,
  remote_entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.cancel(transaction_id: transaction_id, remote_entity_id: remote_entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Transaction-Finished', timeout: timeout) if timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Proxy-Put-Response', timeout: timeout) if remote_entity_id and timeout
  return indication
end

def cfdp_suspend(
  transaction_id:,
  remote_entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.suspend(transaction_id: transaction_id, remote_entity_id: remote_entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Suspended', timeout: timeout) if not remote_entity_id and timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Remote-Suspend-Response', timeout: timeout) if remote_entity_id and timeout
  return indication
end

def cfdp_resume(
  transaction_id:,
  remote_entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.resume(transaction_id: transaction_id, remote_entity_id: remote_entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Resumed', timeout: timeout) if not remote_entity_id and timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Remote-Resume-Response', timeout: timeout) if remote_entity_id and timeout
  return indication
end

def cfdp_report(
  transaction_id:,
  remote_entity_id: nil,
  report_file_name: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.report(transaction_id: transaction_id, remote_entity_id: remote_entity_id, report_file_name: report_file_name, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Report', timeout: timeout) if not remote_entity_id and timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Remote-Report-Response', timeout: timeout) if remote_entity_id and timeout
  return indication
end

def cfdp_indications(
  transaction_id: nil,
  indication_type: nil,
  continuation: nil,
  include_continuation: false,
  timeout: nil,
  limit: 100,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  return cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: indication_type, continuation: continuation, include_continuation: include_continuation, timeout: timeout, limit: limit)
end

def cfdp_directory_listing(
  remote_entity_id:,
  directory_name:,
  directory_file_name:,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)

  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.directory_listing(remote_entity_id: remote_entity_id, directory_name: directory_name, directory_file_name: directory_file_name)
  indications = []
  indications << cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Transaction-Finished', timeout: timeout) if timeout
  indications << cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Directory-Listing-Response', timeout: timeout) if timeout
  return (indications.empty? ? nil : indications)
end

# Helper methods

def cfdp_wait_for_indication(
  api:,
  transaction_id: nil,
  indication_type: nil,
  continuation: nil,
  include_continuation: false,
  timeout: 600,
  limit: 100)

  timeout ||= 0
  start_time = Time.now
  end_time = start_time + timeout
  continuation = '0-0' unless continuation
  done = false
  while not done
    result = api.indications(transaction_id: transaction_id, continuation: continuation, limit: limit)
    continuation = result['continuation']
    indications = result['indications']
    if indications and indications.length > 0
      unless indication_type
        if include_continuation
          return indications, continuation
        else
          return indications
        end
      end
      indications.each do |indication|
        if indication['indication_type'] == indication_type
          if include_continuation
            return indication, continuation
          else
            return indication
          end
        end
      end
    end
    done = true if Time.now >= end_time
    break if done
    if defined? wait
      wait_time = wait(1)
      break if wait_time < 1.0 # User hit go
    else
      sleep(1)
    end
  end
  return nil
end

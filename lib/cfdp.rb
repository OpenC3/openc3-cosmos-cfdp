# encoding: ascii-8bit

# Copyright 2022 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# purchased from OpenC3, Inc.

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
  source_entity_id: nil, # Used to indicate proxy put
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
    closure_requested: closure_requested)
    filestore_requests: filestore_requests,
    fault_handler_overrides: fault_handler_overrides,
    flow_label: flow_label,
    segmentation_control: segmentation_control,
    messages_to_user: messages_to_user,
    source_entity_id: source_entity_id, # Used to indicate proxy put
    scope: $openc3_scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Transaction-Finished', timeout: timeout) if timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Proxy-Put', timeout: timeout) if source_entity_id and timeout
  return indication
end

def cfdp_cancel(
  transaction_id:,
  entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  api.cancel(transaction_id: transaction_id, entity_id: entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Transaction-Finished', timeout: timeout) if timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Proxy-Put-Cancel', timeout: timeout) if entity_id and timeout
  return indication
end

def cfdp_suspend(
  transaction_id:,
  entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  api.suspend(transaction_id: transaction_id, entity_id: entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Suspended', timeout: timeout) if not entity_id and timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Remote-Suspend', timeout: timeout) if entity_id and timeout
  return indication
end

def resume(transaction_id:, entity_id: nil, scope: $openc3_scope)
  transaction_id:,
  entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  api.resume(transaction_id: transaction_id, entity_id: entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Resumed', timeout: timeout) if not entity_id and timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Remote-Resume', timeout: timeout) if entity_id and timeout
  return indication
end

def report(transaction_id:, entity_id: nil, scope: $openc3_scope)
  transaction_id:,
  entity_id: nil,
  timeout: 600,
  microservice_name: 'CFDP',
  prefix: '/cfdp',
  schema: 'http',
  hostname: nil,
  port: 2905,
  url: nil,
  scope: $openc3_scope)
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  api.report(transaction_id: transaction_id, entity_id: entity_id, scope: scope)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Report', timeout: timeout) if not entity_id and timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Remote-Report', timeout: timeout) if entity_id and timeout
  return indication
end

def indications(
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
  entity_id:,
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
  transaction_id = api.directory_listing(entity_id: entity_id, directory_name: directory_name, directory_file_name: directory_file_name)
  indication = nil
  indication = cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Transaction-Finished', timeout: timeout) if timeout
  indication ||= cfdp_wait_for_indication(api: api, transaction_id: transaction_id, indication_type: 'Directory-Listing', timeout: timeout) if timeout
  return indication
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
    done = true if Time.now < end_time
    break if done
    if defined? wait
      wait(1)
    else
      sleep(1)
    end
  end
  return nil
end

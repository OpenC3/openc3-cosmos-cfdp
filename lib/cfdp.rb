# encoding: ascii-8bit

# Copyright 2022 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# purchased from OpenC3, Inc.

require_relative 'cfdp_api'

def cfdp_put_and_wait(destination_entity_id:, source_file_name:, destination_file_name:, closure_requested: nil, timeout: 600, microservice_name: 'CFDP', prefix: '/cfdp', schema: 'http', hostname: nil, port: 2905, url: nil, scope: $openc3_scope)
  start_time = Time.now
  end_time = start_time + timeout
  api = CfdpApi.new(timeout: timeout, microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port, url: url, scope: scope)
  transaction_id = api.put(destination_entity_id: destination_entity_id, source_file_name: source_file_name, destination_file_name: destination_file_name, closure_requested: closure_requested)
  continuation = '0-0'
  done = false
  while Time.now < end_time
    result = api.indications(transaction_id: transaction_id, continuation: continuation, limit: 100)
    continuation = result['continuation']
    indications = result['indications']
    indications.each do |indication|
      puts indication.inspect
      if indication['indication_type'] == 'Transaction-Finished'
        done = true
      end
    end
    break if done
    wait(1)
  end
end

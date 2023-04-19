# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# purchased from OpenC3, Inc.

require 'openc3/utilities/authentication'
require 'openc3/io/json_api'

# Usage:
#
# Note: Recommend using the methods in cfdp.rb rather than this file directly
#
# In ScriptRunner:
# require 'cfdp_api'
# api = CfdpApi.new
# api.put(...)
#
# Outside cluster - Open Source:
# require 'cfdp_api'
# $openc3_scope = 'DEFAULT'
# ENV['OPENC3_API_PASSWORD'] = 'password'
# api = CfdpApi.new(hostname: '127.0.0.1', port: 2900)
# api.put(...)
#
# Outside cluster - Enterprise
# require 'cfdp_api'
# $openc3_scope = 'DEFAULT'
# ENV['OPENC3_KEYCLOAK_URL'] = '127.0.0.1:2900'
# ENV['OPENC3_API_USER'] = 'operator'
# ENV['OPENC3_API_PASSWORD'] = 'operator'
# api = CfdpApi.new(hostname: '127.0.0.1', port: 2900)
# api.put(...)
#
class CfdpApi < OpenC3::JsonApi
  def put(
    destination_entity_id:,
    source_file_name:,
    destination_file_name:,
    transmission_mode: nil,
    closure_requested: nil,
    filestore_requests: [],
    fault_handler_overrides: [],
    flow_label: nil,
    segmentation_control: "NOT_PRESERVED",
    messages_to_user: [],
    source_entity_id: nil, # Used to indicate proxy put
    scope: $openc3_scope)

    begin
      endpoint = "/put"
      data = {
        "destination_entity_id" => destination_entity_id.to_i,
        "source_file_name" => source_file_name,
        "destination_file_name" => destination_file_name,
        "transmission_mode" => transmission_mode,
        "closure_requested" => closure_requested,
        "filestore_requests" => filestore_requests,
        "fault_handler_overrides" => fault_handler_overrides,
        "messages_to_user" => messages_to_user,
        "flow_label" => flow_label,
        "segmentation_control" => segmentation_control
      }
      data["source_entity_id"] = source_entity_id.to_i if source_entity_id
      response = _request('post', endpoint, data: data, scope: scope)
      if response.nil? || response.code != 200
        if response
          raise "CFDP put error: #{response.body}"
        else
          raise "CFDP put failed"
        end
      end
      return response.body
    rescue => error
      raise "CFDP put failed due to #{error.formatted}"
    end
  end

  def cancel(transaction_id:, entity_id: nil, scope: $openc3_scope)
    transaction_id_post(method_name: "cancel", transaction_id: transaction_id, entity_id: entity_id, scope: $openc3_scope)
  end

  def suspend(transaction_id:, entity_id: nil, scope: $openc3_scope)
    transaction_id_post(method_name: "suspend", transaction_id: transaction_id, entity_id: entity_id, scope: $openc3_scope)
  end

  def resume(transaction_id:, entity_id: nil, scope: $openc3_scope)
    transaction_id_post(method_name: "resume", transaction_id: transaction_id, entity_id: entity_id, scope: $openc3_scope)
  end

  def report(transaction_id:, entity_id: nil, scope: $openc3_scope)
    transaction_id_post(method_name: "report", transaction_id: transaction_id, entity_id: entity_id, scope: $openc3_scope)
  end

  def indications(transaction_id: nil, continuation: nil, limit: 100, scope: $openc3_scope)
    begin
      endpoint = "/indications"
      endpoint << ('/' + transaction_id.to_s) if transaction_id
      query = {}
      query[:continuation] = continuation if continuation
      query[:limit] = limit if limit
      response = _request('get', endpoint, query: query, scope: scope)
      if response.nil? || response.code != 200
        if response
          raise "CFDP indications error: #{response.body}"
        else
          raise "CFDP indications failed"
        end
      end
      # Hash of continuation, and indications array
      return JSON.parse(response.body)
    rescue => error
      raise "CFDP indications failed due to #{error.formatted}"
    end
  end

  def directory_listing(entity_id:, directory_name:, directory_file_name:)
    begin
      endpoint = "/directorylisting"
      data = { "entity_id" => entity_id, "directory_name" => directory_name, "directory_file_name" => directory_file_name }
      response = _request('post', endpoint, data: data, scope: scope)
      if response.nil? || response.code != 200
        if response
          raise "CFDP directory listing error: #{response.body}"
        else
          raise "CFDP  directory listing failed"
        end
      end
      return response.body
    rescue => error
      raise "CFDP  directory listing failed due to #{error.formatted}"
    end
  end

  # private

  transaction_id_post(method_name:, transaction_id:, entity_id: nil, scope: $openc3_scope)
    begin
      endpoint = "/#{method_name}"
      data = { "transaction_id" => transaction_id.to_s }
      data['entity_id'] = entity_id if entity_id
      response = _request('post', endpoint, data: data, scope: scope)
      if response.nil? || response.code != 200
        if response
          raise "CFDP #{method_name} error: #{response.body}"
        else
          raise "CFDP #{method_name} failed"
        end
      end
      return response.body
    rescue => error
      raise "CFDP #{method_name} failed due to #{error.formatted}"
    end
  end
end

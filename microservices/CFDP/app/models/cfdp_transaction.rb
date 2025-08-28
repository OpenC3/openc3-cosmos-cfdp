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

require 'openc3/api/api'
require_relative 'cfdp_model'
require_relative 'cfdp_mib'
require_relative 'cfdp_topic'
require_relative 'cfdp_pdu'
require_relative 'cfdp_checksum'
require_relative 'cfdp_null_checksum'
require_relative 'cfdp_crc_checksum'
require 'tempfile'

class CfdpTransaction
  include OpenC3::Api
  attr_reader :id
  attr_reader :frozen
  attr_reader :state
  attr_reader :transaction_status
  attr_reader :progress
  attr_reader :transaction_seq_num
  attr_reader :condition_code
  attr_reader :delivery_code
  attr_reader :file_status
  attr_reader :metadata_pdu_hash
  attr_reader :metadata_pdu_count
  attr_reader :create_time
  attr_reader :complete_time
  attr_accessor :proxy_response_info
  attr_accessor :proxy_response_needed

  def self.build_transaction_id(source_entity_id, transaction_seq_num)
    "#{source_entity_id}__#{transaction_seq_num}"
  end

  def self.get_saved_transaction_ids
    OpenC3::Store.smembers("cfdp_saved_transaction_ids") || []
  end

  def self.clear_saved_transaction_ids
    OpenC3::Store.del("cfdp_saved_transaction_ids")
  end

  def self.has_saved_state?(transaction_id)
    OpenC3::Store.sismember("cfdp_saved_transaction_ids", transaction_id)
  end

  def remove_saved_state
    if @id
      OpenC3::Store.del("cfdp_transaction_state:#{@id}")
      OpenC3::Store.srem("cfdp_saved_transaction_ids", @id)
      OpenC3::Logger.debug("CFDP Transaction #{@id} state removed", scope: ENV['OPENC3_SCOPE'])
    end
  end

  def initialize
    @frozen = false
    @state = "ACTIVE" # ACTIVE, FINISHED, CANCELED, SUSPENDED, ABANDONED
    @transaction_status = "ACTIVE" # UNDEFINED, ACTIVE, TERMINATED, UNRECOGNIZED
    @progress = 0
    @condition_code = "NO_ERROR"
    @delivery_code = nil
    @canceling_entity_id = nil
    @fault_handler_overrides = {}
    @metadata_pdu_hash = nil
    @metadata_pdu_count = 0
    @proxy_response_info = nil
    @proxy_response_needed = false
    @source_file_name = nil
    @destination_file_name = nil
    @create_time = Time.now.utc
    @complete_time = nil
  end

  def as_json(*args)
    result = {
      "id" => @id,
      "frozen" => @frozen,
      "state" => @state,
      "transaction_status" => @transaction_status,
      "progress" => @progress,
      "condition_code" => @condition_code,
      "source_file_name" => @source_file_name,
      "destination_file_name" => @destination_file_name,
      "create_time" => @create_time.iso8601(6)
    }
    result["complete_time"] = @complete_time.iso8601(6) if @complete_time
    return result
  end

  def suspend
    OpenC3::Logger.info("CFDP Suspend Transaction #{@id}", scope: ENV['OPENC3_SCOPE'])
    if @state == "ACTIVE"
      @condition_code = "SUSPEND_REQUEST_RECEIVED"
      @state = "SUSPENDED"
      CfdpTopic.write_indication("Suspended", transaction_id: @id, condition_code: @condition_code) if CfdpMib.source_entity['suspended_indication']
    end
  end

  def resume
    OpenC3::Logger.info("CFDP Resume Transaction #{@id}", scope: ENV['OPENC3_SCOPE'])
    if @state == "SUSPENDED"
      @state = "ACTIVE"
      @condition_code = "NO_ERROR"
      @inactivity_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval']
      CfdpTopic.write_indication("Resumed", transaction_id: @id, progress: @progress) if CfdpMib.source_entity['resume_indication']
    end
  end

  def cancel(canceling_entity_id = nil)
    OpenC3::Logger.info("CFDP Cancel Transaction #{@id}", scope: ENV['OPENC3_SCOPE'])
    if @state != "FINISHED"
      @condition_code = "CANCEL_REQUEST_RECEIVED" if @condition_code == "NO_ERROR"
      if canceling_entity_id
        @canceling_entity_id = canceling_entity_id
      else
        @canceling_entity_id = CfdpMib.source_entity['id']
      end
      @state = "CANCELED"
      @transaction_status = "TERMINATED"
      @complete_time = Time.now.utc
      remove_saved_state
    end
  end

  def abandon
    OpenC3::Logger.info("CFDP Abandon Transaction #{@id}", scope: ENV['OPENC3_SCOPE'])
    if @state != "FINISHED"
      @state = "ABANDONED"
      @transaction_status = "TERMINATED"
      CfdpTopic.write_indication("Abandoned", transaction_id: @id, condition_code: @condition_code, progress: @progress)
      @complete_time = Time.now.utc
      remove_saved_state
    end
  end

  def report
    CfdpTopic.write_indication("Report", transaction_id: @id, status_report: build_report())
  end

  def freeze
    OpenC3::Logger.info("CFDP Freeze Transaction #{@id}", scope: ENV['OPENC3_SCOPE'])
    @freeze = true
  end

  def unfreeze
    OpenC3::Logger.info("CFDP Unfreeze Transaction #{@id}", scope: ENV['OPENC3_SCOPE'])
    @freeze = false
  end

  def build_report
    JSON.generate(as_json(allow_nan: true), allow_nan: true)
  end

  def handle_fault
    OpenC3::Logger.error("CFDP Fault Transaction #{@id}, #{@condition_code}", scope: ENV['OPENC3_SCOPE'])
    if @fault_handler_overrides[@condition_code]
      case @fault_handler_overrides[@condition_code]
      when "ISSUE_NOTICE_OF_CANCELLATION"
        cancel()
      when "ISSUE_NOTICE_OF_SUSPENSION"
        suspend()
      when "IGNORE_ERROR"
        ignore_fault()
      when "ABANDON_TRANSACTION"
        abandon()
      end
    else
      case CfdpMib.source_entity['fault_handler'][@condition_code]
      when "ISSUE_NOTICE_OF_CANCELLATION"
        cancel()
      when "ISSUE_NOTICE_OF_SUSPENSION"
        suspend()
      when "IGNORE_ERROR"
        ignore_fault()
      when "ABANDON_TRANSACTION"
        abandon()
      end
    end
  end

  def ignore_fault
    CfdpTopic.write_indication("Fault", transaction_id: @id, condition_code: @condition_code, progress: @progress)
  end

  def update
    # Default do nothing
  end

  def get_checksum(checksum_type)
    case checksum_type
    when 0, nil # Modular Checksum
      return CfdpChecksum.new
    when 1 # Proximity-1 CRC-32 - Poly: 0x00A00805 - Reference CCSDS-211.2-B-3 - Unsure of correct xor/reflect
      return CfdpCrcChecksum.new(0x00A00805, 0x00000000, false, false)
    when 2 # CRC-32C - Poly: 0x1EDC6F41 - Reference RFC4960
      return CfdpCrcChecksum.new(0x1EDC6F41, 0xFFFFFFFF, true, true)
    when 3 # CRC-32 - Poly: 0x04C11DB7 - Reference Ethernet Frame Check Sequence
      return CfdpCrcChecksum.new(0x04C11DB7, 0xFFFFFFFF, true, true)
    when 15
      return CfdpNullChecksum.new
    else # Unsupported
      return nil
    end
  end

  def save_state
    state_data = {
      'id' => @id,
      'frozen' => @frozen,
      'state' => @state,
      'transaction_status' => @transaction_status,
      'progress' => @progress,
      'transaction_seq_num' => @transaction_seq_num,
      'condition_code' => @condition_code,
      'delivery_code' => @delivery_code,
      'file_status' => @file_status,
      'metadata_pdu_hash' => @metadata_pdu_hash.to_json,
      'metadata_pdu_count' => @metadata_pdu_count,
      'create_time' => @create_time&.iso8601(6),
      'proxy_response_info' => @proxy_response_info,
      'proxy_response_needed' => @proxy_response_needed,
      'canceling_entity_id' => @canceling_entity_id,
      'fault_handler_overrides' => @fault_handler_overrides&.empty? ? nil : @fault_handler_overrides.to_json,
      'source_file_name' => @source_file_name,
      'destination_file_name' => @destination_file_name
    }

    state_data.each do |field, value|
      if value.nil?
        OpenC3::Store.hdel("cfdp_transaction_state:#{@id}", field)
      else
        OpenC3::Store.hset("cfdp_transaction_state:#{@id}", field, value.to_s)
      end
    end

    OpenC3::Store.sadd("cfdp_saved_transaction_ids", @id)
    OpenC3::Logger.debug("CFDP Transaction #{@id} state saved", scope: ENV['OPENC3_SCOPE'])
  end

  def load_state(transaction_id)
    state_data = OpenC3::Store.hgetall("cfdp_transaction_state:#{transaction_id}")
    return false if state_data.empty?

    @id = state_data['id']
    @frozen = state_data['frozen'] == 'true'
    @state = state_data['state'] || 'ACTIVE'
    @transaction_status = state_data['transaction_status'] || 'ACTIVE'
    @progress = state_data['progress']&.to_i || 0
    @transaction_seq_num = state_data['transaction_seq_num']&.to_i
    @condition_code = state_data['condition_code'] || 'NO_ERROR'
    @delivery_code = state_data['delivery_code']
    @file_status = state_data['file_status']
    @metadata_pdu_hash = state_data['metadata_pdu_hash'] ? JSON.parse(state_data['metadata_pdu_hash']) : nil
    @metadata_pdu_count = state_data['metadata_pdu_count']&.to_i || 0
    @create_time = state_data['create_time'] ? Time.parse(state_data['create_time']) : nil
    @complete_time = nil # Completed transactions are not persisted
    @proxy_response_info = state_data['proxy_response_info']
    @proxy_response_needed = state_data['proxy_response_needed'] == 'true'
    @canceling_entity_id = state_data['canceling_entity_id']&.to_i
    @fault_handler_overrides = state_data['fault_handler_overrides'] ? JSON.parse(state_data['fault_handler_overrides']) : {}
    @source_file_name = state_data['source_file_name']
    @destination_file_name = state_data['destination_file_name']

    OpenC3::Logger.debug("CFDP Transaction #{@id} state loaded", scope: ENV['OPENC3_SCOPE'])
    return true
  end

  def cfdp_cmd(entity, target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
    cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
    sleep(entity['cmd_delay']) if entity['cmd_delay']
  end
end

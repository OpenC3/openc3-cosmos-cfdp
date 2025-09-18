# encoding: ascii-8bit

# Copyright 2025 OpenC3, Inc.
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
#
# The development of this software was funded in-part by Sandia National Laboratories.
# See https://github.com/OpenC3/openc3-cosmos-cfdp/pull/12 for details

require_relative 'cfdp_transaction'
require 'base64'

class CfdpReceiveTransaction < CfdpTransaction
  def initialize(pdu_hash)
    super()
    @id = CfdpTransaction.build_transaction_id(pdu_hash["SOURCE_ENTITY_ID"], pdu_hash["SEQUENCE_NUMBER"])
    @transaction_seq_num = pdu_hash["SEQUENCE_NUMBER"]
    @transmission_mode = pdu_hash["TRANSMISSION_MODE"]
    @messages_to_user = []
    @filestore_requests = []
    @tmp_file = nil
    @segments = {}
    @eof_pdu_hash = nil
    @checksum = CfdpNullChecksum.new
    @full_checksum_needed = false
    @file_size = 0
    @file_status = "UNREPORTED"
    @delivery_code = "DATA_COMPLETE"
    @filestore_responses = []
    @nak_timeout = nil
    @nak_timeout_count = 0
    @check_timeout = nil
    @check_timeout_count = 0
    @progress = 0
    @nak_start_of_scope = 0
    @keep_alive_count = 0
    @finished_count = 0
    @source_entity_id = nil
    @inactivity_timeout = nil
    @inactivity_count = 0
    @keep_alive_timeout = nil
    CfdpMib.transactions[@id] = self
    handle_pdu(pdu_hash)
    @inactivity_timeout = Time.now + CfdpMib.entity(@source_entity_id)['keep_alive_interval']
    @keep_alive_timeout = Time.now + CfdpMib.entity(@source_entity_id)['keep_alive_interval'] if @transmission_mode == 'ACKNOWLEDGED' and CfdpMib.entity(@source_entity_id)['enable_keep_alive']
  end

  def check_complete
    return false unless @metadata_pdu_hash and @eof_pdu_hash
    if @eof_pdu_hash["CONDITION_CODE"] != "NO_ERROR" # Canceled
      @state = "CANCELED"
      @transaction_status = "TERMINATED"
      @condition_code = @eof_pdu_hash["CONDITION_CODE"]
      @file_status = "FILE_DISCARDED"
      @delivery_code = "DATA_INCOMPLETE"
      if CfdpMib.entity(@source_entity_id)['incomplete_file_disposition'] == "DISCARD"
        @tmp_file.unlink if @tmp_file
        @tmp_file = nil
      else
        # Keep
        if @tmp_file
          @tmp_file.close
          success = CfdpMib.put_destination_file(@destination_file_name, @tmp_file) # Unlink handled by CfdpMib
          if success
            @file_status = "FILESTORE_SUCCESS"
          else
            @file_status = "FILESTORE_REJECTION"
          end
        end
      end
      notice_of_completion()
      return true
    end

    if @source_file_name and @destination_file_name
      if complete_file_received?
        @tmp_file ||= Tempfile.new('cfdp', binmode: true)

        # Complete
        if @checksum.check(@tmp_file, @eof_pdu_hash['FILE_CHECKSUM'], @full_checksum_needed)
          # Move file to final destination
          @tmp_file.close
          success = CfdpMib.put_destination_file(@destination_file_name, @tmp_file) # Unlink handled by CfdpMib
          if success
            @file_status = "FILESTORE_SUCCESS"
          else
            @file_status = "FILESTORE_REJECTION"
            @condition_code = "FILESTORE_REJECTION"
            handle_fault()
          end
          @delivery_code = "DATA_COMPLETE"
        else
          @tmp_file.unlink
          @file_status = "FILE_DISCARDED"
          @condition_code = "FILE_CHECKSUM_FAILURE"
          handle_fault()
          @delivery_code = "DATA_INCOMPLETE"
        end
        @tmp_file = nil
      else
        # Still waiting on file data
        return false
      end
    end

    # Handle Filestore Requests
    filestore_success = true
    tlvs = @metadata_pdu_hash["TLVS"]
    if tlvs and (@condition_code == "NO_ERROR" or @condition_code == "UNSUPPORTED_CHECKSUM_TYPE")
      tlvs.each do |tlv|
        case tlv['TYPE']
        when 'FILESTORE_REQUEST'
          action_code = tlv["ACTION_CODE"]
          first_file_name = tlv["FIRST_FILE_NAME"]
          second_file_name = tlv["SECOND_FILE_NAME"]
          if filestore_success
            status_code, filestore_message = CfdpMib.filestore_request(action_code, first_file_name, second_file_name)
            filestore_response = {}
            filestore_response['ACTION_CODE'] = action_code
            filestore_response['STATUS_CODE'] = status_code
            filestore_response['FIRST_FILE_NAME'] = first_file_name
            filestore_response['SECOND_FILE_NAME'] = second_file_name
            filestore_response['FILESTORE_MESSAGE'] = filestore_message
            @filestore_responses << filestore_response
            filestore_success = false if status_code != 'SUCCESSFUL'
          else
            filestore_response = {}
            filestore_response['ACTION_CODE'] = action_code
            filestore_response['STATUS_CODE'] = "NOT_PERFORMED"
            filestore_response['FIRST_FILE_NAME'] = first_file_name
            filestore_response['SECOND_FILE_NAME'] = second_file_name
            @filestore_responses << filestore_response
          end
        end
      end
    end

    notice_of_completion()
    return true
  end

  def notice_of_completion
    # Cancel all timeouts
    @check_timeout = nil
    @nak_timeout = nil
    @keep_alive_timeout = nil
    @inactivity_timeout = nil
    @finished_ack_timeout = nil

    destination_entity = CfdpMib.source_entity
    source_entity = CfdpMib.entity(@source_entity_id)
    if source_entity['enable_finished'] and (@metadata_pdu_hash["CLOSURE_REQUESTED"] == "CLOSURE_REQUESTED" or @transmission_mode == "ACKNOWLEDGED")
      begin
        # Lookup outgoing PDU command
        raise "Unknown source entity: #{@metadata_pdu_hash['SOURCE_ENTITY_ID']}" unless source_entity
        target_name, packet_name, item_name = source_entity["cmd_info"]
        raise "cmd_info not defined for source entity: #{@metadata_pdu_hash['SOURCE_ENTITY_ID']}" unless target_name and packet_name and item_name
        @finished_pdu = CfdpPdu.build_finished_pdu(
          source_entity: source_entity,
          transaction_seq_num: @transaction_seq_num,
          destination_entity: destination_entity,
          condition_code: @condition_code,
          segmentation_control: "NOT_PRESERVED",
          transmission_mode: @transmission_mode,
          delivery_code: @delivery_code,
          file_status: @file_status,
          filestore_responses: @filestore_responses,
          fault_location_entity_id: nil)
        cmd_params = {}
        cmd_params[item_name] = @finished_pdu
        cfdp_cmd(source_entity, target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      rescue => err
        abandon() if @state == "CANCELED"
        raise err
      end
      @finished_ack_timeout = Time.now + source_entity['ack_timer_interval'] if @transmission_mode == "ACKNOWLEDGED" and source_entity['enable_acks']
    end

    @state = "FINISHED" unless @state == "CANCELED" or @state == "ABANDONED"
    @transaction_status = "TERMINATED"
    @complete_time = Time.now.utc
    save_state
    OpenC3::Logger.info("CFDP Finished Receive Transaction #{@id}, #{@condition_code}", scope: ENV['OPENC3_SCOPE'])

    if CfdpMib.source_entity['transaction_finished_indication']
      if @filestore_responses.length > 0
        CfdpTopic.write_indication("Transaction-Finished", transaction_id: @id, condition_code: @condition_code, file_status: @file_status, delivery_code: @delivery_code, status_report: @state, filestore_responses: @filestore_responses)
      else
        CfdpTopic.write_indication("Transaction-Finished", transaction_id: @id, condition_code: @condition_code, file_status: @file_status, status_report: @state, delivery_code: @delivery_code)
      end
    end
  end

  def complete_file_received?
    return false unless @file_size
    offset = 0
    while offset
      next_offset = @segments[offset]
      if next_offset
        return true if next_offset == @file_size
      else
        # See if any segments cover the next offset
        @segments.each do |segment_offset, segment_next_offset|
          if offset > segment_offset and offset < segment_next_offset
            # Found
            next_offset = segment_next_offset
            return true if next_offset == @file_size
            break
          end
        end
      end
      offset = next_offset
    end
    return false
  end

  def cancel(canceling_entity_id = nil)
    super(canceling_entity_id)
    notice_of_completion()
  end

  def suspend
    if @transmission_mode == "ACKNOWLEDGED"
      super()
    end
  end

  def update
    if @state != "SUSPENDED"
      if @check_timeout
        if Time.now > @check_timeout
          @check_timeout_count += 1
          if @check_timeout_count < CfdpMib.entity(@source_entity_id)['check_limit']
            @check_timeout = Time.now + CfdpMib.entity(@source_entity_id)['check_interval']
          else
            @condition_code = "CHECK_LIMIT_REACHED"
            handle_fault()
            @check_timeout = nil
          end
        end
      end
      if @nak_timeout
        if Time.now > @nak_timeout
          if complete_file_received?
            @nak_timeout = nil
          else
            send_naks(true)
            @nak_timeout_count += 1
            if @nak_timeout_count < CfdpMib.entity(@source_entity_id)['nak_timer_expiration_limit']
              @nak_timeout = Time.now + CfdpMib.entity(@source_entity_id)['nak_timer_interval']
            else
              @condition_code = "NAK_LIMIT_REACHED"
              handle_fault()
              @nak_timeout = nil
            end
          end
        end
      end
      if @keep_alive_timeout
        if @eof_pdu_hash
          @keep_alive_timeout = nil
        else
          if Time.now > @keep_alive_timeout
            send_keep_alive()
            @keep_alive_count += 1
            @keep_alive_timeout = Time.now + CfdpMib.entity(@source_entity_id)['keep_alive_interval']
          end
        end
      end
      if @inactivity_timeout
        if @eof_pdu_hash
          @inactivity_timeout = nil
        else
          if Time.now > @inactivity_timeout
            @inactivity_count += 1
            if @inactivity_count < CfdpMib.entity(@source_entity_id)['transaction_inactivity_limit']
              @inactivity_timeout = Time.now + CfdpMib.entity(@source_entity_id)['keep_alive_interval']
            else
              @condition_code = "INACTIVITY_DETECTED"
              handle_fault()
            end
          end
        end
      end
      if @finished_ack_timeout
        if @finished_ack_pdu_hash
          @finished_ack_timeout = nil
        else
          if Time.now > @finished_ack_timeout
            source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
            raise "Unknown source entity: #{@metadata_pdu_hash['SOURCE_ENTITY_ID']}" unless source_entity
            target_name, packet_name, item_name = source_entity["cmd_info"]
            raise "cmd_info not defined for source entity: #{@metadata_pdu_hash['SOURCE_ENTITY_ID']}" unless target_name and packet_name and item_name
            cmd_params = {}
            cmd_params[item_name] = @finished_pdu
            cfdp_cmd(source_entity, target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
            @finished_count += 1
            if @finished_count > CfdpMib.entity(@source_entity_id)['ack_timer_expiration_limit']
              # Positive ACK Limit Reached Fault
              @condition_code = "ACK_LIMIT_REACHED"
              handle_fault()
              @finished_ack_timeout = nil
            else
              @finished_ack_timeout = Time.now + CfdpMib.entity(@source_entity_id)['ack_timer_interval']
            end
          end
        end
      end
    end
  end

  def send_keep_alive
    source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
    destination_entity = CfdpMib.source_entity
    target_name, packet_name, item_name = source_entity["cmd_info"]

    keep_alive_pdu = CfdpPdu.build_keep_alive_pdu(
      source_entity: source_entity,
      transaction_seq_num: @transaction_seq_num,
      destination_entity: destination_entity,
      file_size: @file_size,
      segmentation_control: "NOT_PRESERVED",
      transmission_mode: @transmission_mode,
      progress: @progress)
    cmd_params = {}
    cmd_params[item_name] = keep_alive_pdu
    cfdp_cmd(source_entity, target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
  end

  def send_naks(force = false)
    source_entity = CfdpMib.entity(@source_entity_id)
    destination_entity = CfdpMib.source_entity
    target_name, packet_name, item_name = source_entity["cmd_info"]

    segment_requests = []
    segment_requests << [0, 0] unless @metadata_pdu_hash

    # TODO: I don't see the metadata_pdu_hash being used anywhere
    # past this point. The Metadata holds the file size so how can
    # we know if we haven't received segments if we never check
    # the original request size?

    if @eof_pdu_hash
      final_end_of_scope = @file_size
    else
      final_end_of_scope = @progress
    end

    if force
      offset = 0
    else
      offset = @nak_start_of_scope
    end
    sorted_segments = @segments.to_a.sort {|a,b| a[0] <=> b[0]}
    index = 0
    sorted_segments.each do |start_offset, end_offset|
      break if end_offset > offset
      index += 1
    end
    sorted_segments = sorted_segments[index..-1]
    while (offset < final_end_of_scope) and sorted_segments.length > 0
      found = false
      sorted_segments.each do |start_offset, end_offset|
        if offset >= start_offset and offset < end_offset
          # Offset found - move to end offset
          offset = end_offset
          found = true
          break
        end
      end
      unless found
        # Need a segment request up to first sorted segment
        segment_requests << [offset, sorted_segments[0][0]]
        offset = sorted_segments[0][1]
      end
      sorted_segments = sorted_segments[1..-1]
    end
    if offset < final_end_of_scope
      segment_requests << [offset, final_end_of_scope]
    end

    # Calculate max number of segments in a single NAK PDU
    if force
      start_of_scope = 0
    else
      start_of_scope = @nak_start_of_scope
    end
    max_segments = (source_entity['maximum_file_segment_length'] / 8) - 2 # Minus 2 handles scope fields
    while true
      num_segments = segment_requests.length
      if num_segments > max_segments
        num_segments = max_segments
      end
      current_segment_requests = segment_requests[0..(num_segments - 1)]
      if current_segment_requests.length == segment_requests.length
        if @eof_pdu_hash
          end_of_scope = @file_size
        else
          end_of_scope = @progress
        end
        @nak_start_of_scope = end_of_scope
      else
        end_of_scope = current_segment_requests[-1][1]
      end
      if start_of_scope != end_of_scope
        nak_pdu = CfdpPdu.build_nak_pdu(
          source_entity: source_entity,
          transaction_seq_num: @transaction_seq_num,
          destination_entity: destination_entity,
          file_size: @file_size,
          segmentation_control: "NOT_PRESERVED",
          transmission_mode: @transmission_mode,
          start_of_scope: start_of_scope,
          end_of_scope: end_of_scope,
          segment_requests: current_segment_requests)
        segment_requests = segment_requests[num_segments..-1]
        start_of_scope = end_of_scope
        cmd_params = {}
        cmd_params[item_name] = nak_pdu
        cfdp_cmd(source_entity, target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      end
      break if segment_requests.length <= 0
    end
  end

  def handle_pdu(pdu_hash)
    source_entity = CfdpMib.entity(@source_entity_id)
    @inactivity_timeout = Time.now + source_entity['keep_alive_interval'] if source_entity

    case pdu_hash["DIRECTIVE_CODE"]
    when "METADATA"
      @metadata_pdu_count += 1
      if @metadata_pdu_hash # Discard repeats
        save_state if @id
        return
      end
      @metadata_pdu_hash = pdu_hash
      @source_entity_id = @metadata_pdu_hash['SOURCE_ENTITY_ID']
      kw_args = {}
      tlvs = pdu_hash['TLVS']
      if tlvs
        tlvs.each do |tlv|
          case tlv["TYPE"]
          when "FILESTORE_REQUEST"
            filestore_request = {}
            filestore_request["ACTION_CODE"] = tlv["ACTION_CODE"]
            filestore_request["FIRST_FILE_NAME"] = tlv["FIRST_FILE_NAME"]
            filestore_request["SECOND_FILE_NAME"] = tlv["SECOND_FILE_NAME"] if tlv["SECOND_FILE_NAME"]
            @filestore_requests << filestore_request

          when "MESSAGE_TO_USER"
            @messages_to_user << tlv["MESSAGE_TO_USER"]

          when "FAULT_HANDLER_OVERRIDE"
            @fault_handler_overrides[tlv["CONDITION_CODE"]] = tlv["HANDLER_CODE"]

          when "FLOW_LABEL"
            kw_args[:flow_label] = tlv["FLOW_LABEL"]
          end
        end
      end
      kw_args[:filestore_requests] = @filestore_requests unless @filestore_requests.empty?
      kw_args[:messages_to_user] = @messages_to_user unless @messages_to_user.empty?
      kw_args[:fault_handler_overrides] = @fault_handler_overrides unless @fault_handler_overrides.empty?

      kw_args[:transaction_id] = @id
      kw_args[:source_entity_id] = @metadata_pdu_hash['SOURCE_ENTITY_ID']

      @file_size = @metadata_pdu_hash['FILE_SIZE']
      kw_args[:file_size] = @file_size

      @source_file_name = nil
      if @metadata_pdu_hash['SOURCE_FILE_NAME'] and @metadata_pdu_hash['SOURCE_FILE_NAME'].length > 0
        @source_file_name = @metadata_pdu_hash['SOURCE_FILE_NAME']
        kw_args[:source_file_name] = @source_file_name
      end

      @destination_file_name = nil
      if @metadata_pdu_hash['DESTINATION_FILE_NAME'] and @metadata_pdu_hash['DESTINATION_FILE_NAME'].length > 0
        @destination_file_name = @metadata_pdu_hash['DESTINATION_FILE_NAME']
        kw_args[:destination_file_name] = @destination_file_name
      end

      CfdpTopic.write_indication("Metadata-Recv", **kw_args)

      checksum_type = @metadata_pdu_hash["CHECKSUM_TYPE"]
      checksum_type ||= 0 # For version 0
      @checksum = get_checksum(checksum_type)
      unless @checksum
        # Use Null checksum if checksum type not available
        @condition_code = "UNSUPPORTED_CHECKSUM_TYPE"
        handle_fault()
        @checksum = CfdpNullChecksum.new
      end

    when "EOF"
      @eof_pdu_hash = pdu_hash

      # Check file size fault
      @file_size = @eof_pdu_hash["FILE_SIZE"]
      if @progress > @file_size
        @condition_code = "FILE_SIZE_ERROR"
        handle_fault()
      end

      CfdpTopic.write_indication("EOF-Recv", transaction_id: @id) if CfdpMib.source_entity['eof_recv_indication']

      destination_entity = CfdpMib.source_entity
      source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
      if @transmission_mode == "ACKNOWLEDGED" and source_entity['enable_acks']
        target_name, packet_name, item_name = source_entity["cmd_info"]
        # Ack EOF PDU
        ack_pdu = CfdpPdu.build_ack_pdu(
          source_entity: source_entity,
          transaction_seq_num: @transaction_seq_num,
          destination_entity: destination_entity,
          segmentation_control: "NOT_PRESERVED",
          transmission_mode: @transmission_mode,
          condition_code: @eof_pdu_hash["CONDITION_CODE"],
          ack_directive_code: "EOF",
          transaction_status: "ACTIVE")
        cmd_params = {}
        cmd_params[item_name] = ack_pdu
        cfdp_cmd(source_entity, target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      end

      # Note: This also handles canceled
      complete = check_complete()
      if complete
        send_naks(true) if destination_entity['enable_eof_nak']
      else
        @check_timeout = Time.now + source_entity['check_interval']
        @progress = @file_size
        send_naks() if destination_entity['immediate_nak_mode'] or destination_entity['enable_eof_nak']
        @nak_timeout = Time.now + source_entity['nak_timer_interval']
      end

    when "NAK", "FINISHED", "KEEP_ALIVE"
      # Unexpected - Ignore

    when "ACK"
      @finished_ack_pdu_hash = pdu_hash
      @finished_ack_timeout = nil

    when "PROMPT"
      @prompt_pdu_hash = pdu_hash
      unless @eof_pdu_hash
        if @prompt_pdu_hash['RESPONSE_REQUIRED'] == 'NAK'
          send_naks()
        else
          send_keep_alive()
        end
      end

    else # File Data
      @source_entity_id = @metadata_pdu_hash['SOURCE_ENTITY_ID']

      @tmp_file ||= Tempfile.new('cfdp', binmode: true)
      offset = pdu_hash['OFFSET']
      file_data = pdu_hash['FILE_DATA']
      progress = offset + file_data.length

      need_send_naks = false
      if @transmission_mode == "ACKNOWLEDGED" and CfdpMib.entity(@source_entity_id)['immediate_nak_mode']
        need_send_naks = true unless @metadata_pdu_hash
        need_send_naks = true if offset != @progress and @progress < offset
      end

      @progress = progress if progress > @progress

      # Ignore repeated segments
      if !@segments[offset] or @segments[offset] != progress
        if @file_size and progress > @file_size
          @condition_code = "FILE_SIZE_ERROR"
          handle_fault()
        else
          @full_checksum_needed = true unless @metadata_pdu_hash
          @checksum.add(offset, file_data)
          @segments[offset] = offset + file_data.length
          @tmp_file.seek(offset, IO::SEEK_SET)
          @tmp_file.write(file_data)
        end
        check_complete()

        CfdpTopic.write_indication("File-Segment-Recv", transaction_id: @id, offset: offset, length: file_data.length) if CfdpMib.source_entity['file_segment_recv_indication']
      end

      send_naks() if need_send_naks
    end

    save_state if @id
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
      'metadata_pdu_hash' => @metadata_pdu_hash,
      'metadata_pdu_count' => @metadata_pdu_count,
      'create_time' => @create_time&.iso8601(6),
      'proxy_response_info' => @proxy_response_info,
      'proxy_response_needed' => @proxy_response_needed,
      'canceling_entity_id' => @canceling_entity_id,
      'fault_handler_overrides' => @fault_handler_overrides,
      'source_file_name' => @source_file_name,
      'destination_file_name' => @destination_file_name,
      'transmission_mode' => @transmission_mode,
      'messages_to_user' => @messages_to_user,
      'filestore_requests' => @filestore_requests,
      'tmp_file_path' => @tmp_file&.path,
      'segments' => @segments,
      'eof_pdu_hash' => @eof_pdu_hash,
      'checksum_type' => @checksum.class.name,
      'full_checksum_needed' => @full_checksum_needed,
      'file_size' => @file_size,
      'filestore_responses' => @filestore_responses,
      'nak_timeout' => @nak_timeout&.iso8601(6),
      'nak_timeout_count' => @nak_timeout_count,
      'check_timeout' => @check_timeout&.iso8601(6),
      'check_timeout_count' => @check_timeout_count,
      'nak_start_of_scope' => @nak_start_of_scope,
      'keep_alive_count' => @keep_alive_count,
      'finished_count' => @finished_count,
      'source_entity_id' => @source_entity_id,
      'inactivity_timeout' => @inactivity_timeout&.iso8601(6),
      'inactivity_count' => @inactivity_count,
      'keep_alive_timeout' => @keep_alive_timeout&.iso8601(6),
      'finished_ack_timeout' => @finished_ack_timeout&.iso8601(6),
      'finished_pdu' => @finished_pdu,
      'finished_ack_pdu_hash' => @finished_ack_pdu_hash,
      'prompt_pdu_hash' => @prompt_pdu_hash
    }
    state_data.compact!

    # Store as Base64-encoded Marshal dump to handle all data types safely
    serialized_data = Base64.strict_encode64(Marshal.dump(state_data))
    OpenC3::Store.set("#{self.class.redis_key_prefix}cfdp_transaction_state:#{@id}", serialized_data)
    OpenC3::Store.sadd("#{self.class.redis_key_prefix}cfdp_saved_transaction_ids", @id)
  end

  def load_state(transaction_id)
    serialized_data = OpenC3::Store.get("#{self.class.redis_key_prefix}cfdp_transaction_state:#{transaction_id}")
    return false unless serialized_data

    begin
      state_data = Marshal.load(Base64.strict_decode64(serialized_data))
    rescue => e
      OpenC3::Logger.error("CFDP Transaction #{transaction_id} failed to deserialize state: #{e.message}", scope: ENV['OPENC3_SCOPE'])
      return false
    end

    # Load base state
    @id = state_data['id']
    @frozen = state_data['frozen']
    @state = state_data['state'] || 'ACTIVE'
    @transaction_status = state_data['transaction_status'] || 'ACTIVE'
    @progress = state_data['progress'] || 0
    @transaction_seq_num = state_data['transaction_seq_num']
    @condition_code = state_data['condition_code'] || 'NO_ERROR'
    @delivery_code = state_data['delivery_code']
    @file_status = state_data['file_status']
    @metadata_pdu_hash = state_data['metadata_pdu_hash']
    @metadata_pdu_count = state_data['metadata_pdu_count'] || 0
    @create_time = state_data['create_time'] ? Time.parse(state_data['create_time']) : nil
    @complete_time = nil # Completed transactions are not persisted
    @proxy_response_info = state_data['proxy_response_info']
    @proxy_response_needed = state_data['proxy_response_needed']
    @canceling_entity_id = state_data['canceling_entity_id']
    @fault_handler_overrides = state_data['fault_handler_overrides'] || {}
    @source_file_name = state_data['source_file_name']
    @destination_file_name = state_data['destination_file_name']

    # Load receive-specific state
    @transmission_mode = state_data['transmission_mode']
    @messages_to_user = state_data['messages_to_user'] || []
    @filestore_requests = state_data['filestore_requests'] || []

    if state_data['tmp_file_path']
      begin
        @tmp_file = File.open(state_data['tmp_file_path'], 'r+b')
      rescue
        @tmp_file = nil
      end
    else
      @tmp_file = nil
    end

    @segments = state_data['segments'] || {}
    @eof_pdu_hash = state_data['eof_pdu_hash']

    case state_data['checksum_type']
    when 'CfdpChecksum'
      @checksum = CfdpChecksum.new
    when 'CfdpNullChecksum'
      @checksum = CfdpNullChecksum.new
    when 'CfdpCrcChecksum'
      @checksum = CfdpCrcChecksum.new(0, 0, false, false)
    else
      @checksum = CfdpNullChecksum.new
    end

    @full_checksum_needed = state_data['full_checksum_needed']
    @file_size = state_data['file_size'] || 0
    @filestore_responses = state_data['filestore_responses'] || []
    @nak_timeout = state_data['nak_timeout'] ? Time.parse(state_data['nak_timeout']) : nil
    @nak_timeout_count = state_data['nak_timeout_count'] || 0
    @check_timeout = state_data['check_timeout'] ? Time.parse(state_data['check_timeout']) : nil
    @check_timeout_count = state_data['check_timeout_count'] || 0
    @nak_start_of_scope = state_data['nak_start_of_scope'] || 0
    @keep_alive_count = state_data['keep_alive_count'] || 0
    @finished_count = state_data['finished_count'] || 0
    @source_entity_id = state_data['source_entity_id']
    @inactivity_timeout = state_data['inactivity_timeout'] ? Time.parse(state_data['inactivity_timeout']) : nil
    @inactivity_count = state_data['inactivity_count'] || 0
    @keep_alive_timeout = state_data['keep_alive_timeout'] ? Time.parse(state_data['keep_alive_timeout']) : nil
    @finished_ack_timeout = state_data['finished_ack_timeout'] ? Time.parse(state_data['finished_ack_timeout']) : nil
    @finished_pdu = state_data['finished_pdu']
    @finished_ack_pdu_hash = state_data['finished_ack_pdu_hash']
    @prompt_pdu_hash = state_data['prompt_pdu_hash']

    OpenC3::Logger.info("CFDP Transaction #{@id} state loaded", scope: ENV['OPENC3_SCOPE'])
    return true
  end
end

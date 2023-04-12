require_relative 'cfdp_transaction'

class CfdpReceiveTransaction < CfdpTransaction

  def initialize(pdu_hash)
    super()
    @id = self.class.build_transaction_id(pdu_hash["SOURCE_ENTITY_ID"], pdu_hash["SEQUENCE_NUMBER"])
    @transaction_seq_num = pdu_hash["SEQUENCE_NUMBER"]
    @transmission_mode = pdu_hash["TRANSMISSION_MODE"]
    @messages_to_user = []
    @flow_label = nil
    @filestore_requests = []
    @source_file_name = nil
    @destination_file_name = nil
    @tmp_file = nil
    @segments = {}
    @metadata_pdu_hash = nil
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
    @inactivity_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval']
    @inactivity_count = 0
    @keep_alive_timeout = nil
    @keep_alive_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval'] if @transmission_mode == 'ACKNOWLEDGED'
    @keep_alive_count = 0
    CfdpMib.transactions[@id] = self
    handle_pdu(pdu_hash)
  end

  def self.build_transaction_id(source_entity_id, transaction_seq_num)
    "#{source_entity_id}__#{transaction_seq_num}"
  end

  def check_complete
    return false unless @metadata_pdu_hash and @eof_pdu_hash
    if @eof_pdu_hash["CONDITION_CODE"] != "NO_ERROR" # Canceled
      @status = "CANCELED"
      @condition_code = @eof_pdu_hash["CONDITION_CODE"]
      @file_status = "FILE_DISCARDED"
      @delivery_code = "DATA_INCOMPLETE"
      if CfdpMib.source_entity['incomplete_file_disposition'] == "DISCARD"
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

    if @source_file_name and @destination_file_name and @tmp_file
      if complete_file_received?
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

    if @metadata_pdu_hash["CLOSURE_REQUESTED"] == "CLOSURE_REQUESTED" or @transmission_mode == "ACKNOWLEDGED"
      begin
        # Lookup outgoing PDU command
        destination_entity = CfdpMib.source_entity
        source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
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
        cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      rescue => err
        abandon() if @status == "CANCELED"
        raise err
      end
      @finished_ack_timeout = Time.now + CfdpMib.source_entity['ack_timer_interval'] if @transmission_mode == "ACKNOWLEDGED"
    end

    @status = "FINISHED" unless @status == "CANCELED" or @status == "ABANDONED"

    if @filestore_responses.length > 0
      CfdpTopic.write_indication("Transaction-Finished", transaction_id: @id, condition_code: @condition_code, file_status: @file_status, delivery_code: @delivery_code, status_report: @status, filestore_responses: @filestore_responses)
    else
      CfdpTopic.write_indication("Transaction-Finished", transaction_id: @id, condition_code: @condition_code, file_status: @file_status, status_report: @status, delivery_code: @delivery_code)
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

  def cancel(entity_id = nil)
    super(entity_id)
    notice_of_completion()
  end

  def suspend
    if @transmission_mode == "ACKNOWLEDGED"
      super()
    end
  end

  def update
    if @status != "SUSPENDED"
      if @check_timeout
        if Time.now > @check_timeout
          @check_timeout_count += 1
          if @check_timeout_count < CfdpMib['check_limit']
            @check_timeout = Time.now + CfdpMib.source_entity['check_interval']
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
            if @nak_timeout_count < CfdpMib['nak_timer_expiration_limit']
              @nak_timeout = Time.now + CfdpMib.source_entity['nak_timer_interval']
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
            @keep_alive_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval']
          end
        end
      end
      if @inactivity_timeout
        if @eof_pdu_hash
          @inactivity_timeout = nil
        else
          if Time.now > @inactivity_timeout
            @inactivity_count += 1
            if @inactivity_count < CfdpMib.source_entity['transaction_inactivity_limit']
              @inactivity_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval']
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
            cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
            @finished_count += 1
            if @finished_count > CfdpMib.source_entity['ack_timer_expiration_limit']
              # Positive ACK Limit Reached Fault
              @condition_code = "ACK_LIMIT_REACHED"
              handle_fault()
              @finished_ack_timeout = nil
            else
              @finished_ack_timeout = Time.now + CfdpMib.source_entity['ack_timer_interval']
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
    cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
  end

  def send_naks(force = false)
    source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
    destination_entity = CfdpMib.source_entity
    target_name, packet_name, item_name = source_entity["cmd_info"]

    segment_requests = []
    segment_requests << [0, 0] unless @metadata_pdu_hash

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
        sorted_segments = sorted_segments[1..-1]
      end
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
        cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      end
      break if segment_requests.length <= 0
    end
  end

  def handle_pdu(pdu_hash)
    @inactivity_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval']

    case pdu_hash["DIRECTIVE_CODE"]
    when "METADATA"
      return if @metadata_pdu_hash # Discard repeats
      @metadata_pdu_hash = pdu_hash
      kw_args = {}
      tlvs = pdu_hash['TLVS']
      if tlvs
        tlvs.each do |tlv|
          case tlv["TYPE"]
          when "FILESTORE_REQUEST"
            filestore_request = {}
            filestore_request["ACTION_CODE"] = tlv["ACTION_CODE"]
            filestore_request["FIRST_FILE_NAME"] = tlv["FIRST_FILE_NAME"]
            filestore_request["SECOND_FILE_NAME"] = tlv["SECOND_FILE_NAME"]
            @filestore_requests << filestore_request

          when "MESSAGE_TO_USER"
            @messages_to_user << tlv["MESSAGE_TO_USER"]
            kw_args[:messages_to_user] = @messages_to_user

          when "FAULT_HANDLER_OVERRIDE"
            @fault_handler_overrides[tlv["CONDITION_CODE"]] = tlv["HANDLER_CODE"]

          when "FLOW_LABEL"
            @flow_label = tlv["FLOW_LABEL"]
          end
        end
      end
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

      @checksum = get_checksum(@metadata_pdu_hash["CHECKSUM_TYPE"])
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

      CfdpTopic.write_indication("EOF-Recv", transaction_id: @id)

      if @transmission_mode == "ACKNOWLEDGED"
        source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
        destination_entity = CfdpMib.source_entity
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
        cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      end

      # Note: This also handles canceled
      complete = check_complete()
      unless complete
        @check_timeout = Time.now + CfdpMib.source_entity['check_interval']
        @progress = @file_size
        send_naks() if destination_entity['immediate_nak_mode']
        @nak_timeout = Time.now + CfdpMib.source_entity['nak_timer_interval']
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
      @tmp_file ||= Tempfile.new('cfdp')
      offset = pdu_hash['OFFSET']
      file_data = pdu_hash['FILE_DATA']
      progress = offset + file_data.length

      need_send_naks = false
      if @transmission_mode == "ACKNOWLEDGED" and CfdpMib.source_entity['immediate_nak_mode']
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

        CfdpTopic.write_indication("File-Segment-Recv", transaction_id: @id, offset: offset, length: file_data.length)
      end

      send_naks() if need_send_naks
    end
  end
end

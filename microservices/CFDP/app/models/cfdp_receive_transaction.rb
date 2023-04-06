require_relative 'cfdp_transaction'

class CfdpReceiveTransaction < CfdpTransaction

  def initialize(pdu_hash)
    super()
    @id = self.class.build_transaction_id(pdu_hash["SOURCE_ENTITY_ID"], pdu_hash["SEQUENCE_NUMBER"])
    @transmission_mode = pdu_hash["TRANSMISSION_MODE"]
    @messages_to_user = []
    @flow_label = nil
    @fault_handler_overrides = {}
    @filestore_requests = []
    @source_file_name = nil
    @destination_file_name = nil
    @tmp_file = nil
    @segments = {}
    @metadata_pdu_hash = nil
    @eof_pdu_hash = nil
    @checksum = CfdpNullChecksum.new
    @full_checksum_needed = false
    @file_size = nil
    @filestore_responses = []
    @check_timeout = nil
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
      @tmp_file.unlink if @tmp_file
      @tmp_file = nil
      notice_of_completion()
      return true
    end
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
        end
        @delivery_code = "DATA_COMPLETE"
      else
        @tmp_file.unlink
        @file_status = "FILE_DISCARDED"
        @condition_code = "FILE_CHECKSUM_FAILURE"
        @delivery_code = "DATA_INCOMPLETE"
        # TODO: Handle different fault handlers here
      end
      @tmp_file = nil

      # Handle Filestore Requests
      filestore_success = true
      tlvs = @metadata_pdu_hash["TLVS"]
      if tlvs
        tlvs.each do |tlv|
          case tlv['TLV_TYPE']
          when 'FILESTORE_REQUEST'
            if filestore_success
              action_code = tlv["ACTION_CODE"]
              first_file_name = tlv["FIRST_FILE_NAME"]
              second_file_name = tlv["SECOND_FILE_NAME"]
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
    return false
  end

  def notice_of_completion
    if @metadata_pdu_hash["CLOSURE_REQUESTED"] == "CLOSURE_REQUESTED"
      # Lookup outgoing PDU command
      destination_entity = CfdpMib.source_entity
      source_entity = CfdpMib.entity(@metadata_pdu_hash['SOURCE_ENTITY_ID'])
      raise "Unknown source entity: #{@metadata_pdu_hash['SOURCE_ENTITY_ID']}" unless source_entity
      target_name, packet_name, item_name = source_entity["cmd_info"]
      raise "cmd_info not defined for source entity: #{@metadata_pdu_hash['SOURCE_ENTITY_ID']}" unless target_name and packet_name and item_name
      finished_pdu = CfdpPdu.build_finished_pdu(
        source_entity: source_entity,
        transaction_seq_num: @metadata_pdu_hash["SEQUENCE_NUMBER"],
        destination_entity: destination_entity,
        condition_code: @condition_code,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: @transmission_mode,
        delivery_code: @delivery_code,
        file_status: @file_status,
        filestore_responses: @filestore_responses,
        fault_location_entity_id: nil)
      cmd_params = {}
      cmd_params[item_name] = finished_pdu
      cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
    end

    @status = "FINISHED" unless @status == "CANCELED"

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

  def update
    if @check_timeout
      if Time.now > @check_timeout
        # TODO: Handle timeout - Check Limit Reached Fault
        # TODO: What to do with finished transactions?
      end
    end
  end

  def handle_pdu(pdu_hash)
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

      if @metadata_pdu_hash['SOURCE_FILE_NAME'] and @metadata_pdu_hash['SOURCE_FILE_NAME'].length > 0
        @source_file_name = @metadata_pdu_hash['SOURCE_FILE_NAME']
        kw_args[:source_file_name] = @source_file_name
      end

      if @metadata_pdu_hash['DESTINATION_FILE_NAME'] and @metadata_pdu_hash['DESTINATION_FILE_NAME'].length > 0
        @destination_file_name = @metadata_pdu_hash['DESTINATION_FILE_NAME']
        kw_args[:destination_file_name] = @destination_file_name
      end

      CfdpTopic.write_indication("Metadata-Recv", **kw_args)

      @checksum = get_checksum(@metadata_pdu_hash["CHECKSUM_TYPE"])
      unless @checksum
        # Use Null checksum if checksum type not available
        @condition_code = "UNSUPPORTED_CHECKSUM_TYPE"
        @checksum = CfdpNullChecksum.new
      end

    when "EOF"
      @eof_pdu_hash = pdu_hash

      # Check file size fault
      @file_size = @eof_pdu_hash["FILE_SIZE"]
      if @progress > @file_size
        @condition_code = "FILE_SIZE_ERROR"
      end

      CfdpTopic.write_indication("EOF-Recv", transaction_id: @id)
      complete = check_complete()
      unless complete
        @check_timeout = Time.now + CfdpMib.source_entity['check_limit']
      end

    when "NAK", "FINISHED", "KEEP_ALIVE"
      # Unexpected - Ignore

    when "ACK"

    when "PROMPT"

    else # File Data
      @tmp_file ||= Tempfile.new('cfdp')
      offset = pdu_hash['OFFSET']
      file_data = pdu_hash['FILE_DATA']
      progress = offset + file_data.length
      @progress = progress if progress > @progress

      # Ignore repeated segments
      if !@segments[offset] or @segments[offset] != progress
        if @file_size and progress > @file_size
          @condition_code = "FILE_SIZE_ERROR"
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
    end
  end
end
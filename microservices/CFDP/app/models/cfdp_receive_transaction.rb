require 'openc3/api/api'
require_relative 'cfdp_topic'
require_relative 'cfdp_mib'
require_relative 'cfdp_checksum'
require_relative 'cfdp_null_checksum'
require_relative 'cfdp_pdu'
require 'tempfile'

class CfdpReceiveTransaction
  include OpenC3::Api
  attr_reader :id

  def initialize(metadata_pdu_hash)
    @metadata_pdu_hash = metadata_pdu_hash
    @id = self.class.build_transaction_id(metadata_pdu_hash["SOURCE_ENTITY_ID"], metadata_pdu_hash["SEQUENCE_NUMBER"])
    @transmission_mode = metadata_pdu_hash["TRANSMISSION_MODE"]
    @messages_to_user = []
    @flow_label = nil
    @fault_handler_overrides = {}
    @filestore_requests = []
    kw_args = {}
    tlvs = metadata_pdu_hash['TLVS']
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

    @tmp_file = nil
    @segments = {}
    @condition_code = "NO_ERROR"
    @eof_pdu_hash = nil
    @checksum_type = @metadata_pdu_hash["CHECKSUM_TYPE"]
    @checksum = get_checksum(@checksum_type)
  end

  def self.build_transaction_id(source_entity_id, transaction_seq_num)
    "#{source_entity_id}__#{transaction_seq_num}"
  end

  def check_complete
    return false unless @eof_pdu_hash
    return true if @eof_pdu_hash["CONDITION_CODE"] != "NO_ERROR" # Canceled
    offset = 0
    while offset
      next_offset = @segments[offset]
      if next_offset == @file_size
        # Complete
        if @checksum.check(@eof_pdu_hash['FILE_CHECKSUM'])
          # Move file to final destination
          @tmp_file.close
          success = CfdpMib.put_destination_file(@destination_file_name, @tmp_file)
          if success
            file_status = "FILESTORE_SUCCESS"
          else
            file_status = "FILESTORE_REJECTION"
          end
          delivery_code = "DATA_COMPLETE"
        else
          @tmp_file.unlink
          file_status = "FILE_DISCARDED"
          @condition_code = "FILE_CHECKSUM_FAILURE"
          delivery_code = "DATA_INCOMPLETE"
        end

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
            file_size: @file_size,
            condition_code: @condition_code,
            segmentation_control: "NOT_PRESERVED",
            transmission_mode: @transmission_mode,
            delivery_code: delivery_code,
            file_status: file_status,
            filestore_responses: [],
            fault_location_entity_id: nil)
          cmd_params = {}
          cmd_params[item_name] = finished_pdu
          cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
        end

        return true
      end
      if offset != next_offset
        offset = next_offset
      else
        return false
      end
    end
    return false
  end

  def handle_pdu(pdu_hash)
    case pdu_hash["DIRECTIVE_CODE"]
    when "EOF"
      @eof_pdu_hash = pdu_hash
      check_complete()
      CfdpTopic.write_indication("EOF-Recv", transaction_id: @id)

    when "FINISHED"

    when "ACK"

    when "METADATA"
      raise "METADATA unexpected by handle_pdu"

    when "NAK"

    when "PROMPT"

    when "KEEP_ALIVE"

    else # File Data
      @tmp_file ||= Tempfile.new('cfdp')
      offset = pdu_hash['OFFSET']
      file_data = pdu_hash['FILE_DATA']
      if !@segments[offset] or segments[offset] != (offset + file_data.length)
        if (offset + file_data.length) > @file_size
          @condition_code = "FILE_SIZE_ERROR"
        else
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

  # private

  def get_checksum(checksum_type)
    if checksum_type == 0
      return CfdpChecksum.new
    else
      return NullChecksum.new
    end
  end
end
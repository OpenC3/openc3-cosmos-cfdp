# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

require 'openc3/packets/packet'
require 'openc3/utilities/crc'

class CfdpPdu < OpenC3::Packet
  # Table 5-4: Directive Codes
  DIRECTIVE_CODES = {
    "EOF" => 4,
    "FINISHED" => 5,
    "ACK" => 6,
    "METADATA" => 7,
    "NAK" => 8,
    "PROMPT" => 9,
    "KEEP_ALIVE" => 0x0C
  }

  # Table 5-5: Condition Codes
  CONDITION_CODES = {
    "NO_ERROR" => 0,
    "ACK_LIMIT_REACHED" => 1,
    "KEEP_ALIVE_LIMIT_REACHED" => 2,
    "INVALID_TRANSMISSION_MODE" => 3,
    "FILESTORE_REJECTION" => 4,
    "FILE_CHECKSUM_FAILURE" => 5,
    "FILE_SIZE_ERROR" => 6,
    "NAK_LIMIT_REACHED" => 7,
    "INACTIVITY_DETECTED" => 8,
    "INVALID_FILE_STRUCTURE" => 9,
    "CHECK_LIMIT_REACHED" => 10,
    "UNSUPPORTED_CHECKSUM_TYPE" => 11,
    "SUSPEND_REQUEST_RECEIVED" => 14,
    "CANCEL_REQUEST_RECEIVED" => 15
  }

  # Table 5-7: Finished PDU Contents
  DELIVERY_CODES = {
    "DATA_COMPLETE" => 0,
    "DATA_INCOMPLETE" => 1
  }

  # Table 5-7: Finished PDU Contents
  FILE_STATUS_CODES = {
    "FILE_DISCARDED" => 0,
    "FILESTORE_REJECTION" => 1,
    "FILESTORE_SUCCESS" => 2,
    "UNREPORTED" => 3
  }

  # Paragraph 5.2.4 ACK PDU
  TRANSACTION_STATUS_CODES = {
    "UNDEFINED" => 0,
    "ACTIVE" => 1,
    "TERMINATED" => 2,
    "UNRECOGNIZED" => 3
  }

  # Table 5-16: Filestore Request TLV Action Codes
  ACTION_CODES = {
    "CREATE_FILE" => 0,
    "DELETE_FILE" => 1,
    "RENAME_FILE" => 2,
    "APPEND_FILE" => 3,
    "REPLACE_FILE" => 4,
    "CREATE_DIRECTORY" => 5,
    "REMOVE_DIRECTORY" => 6,
    "DENY_FILE" => 7,
    "DENY_DIRECTORY" => 8
  }

  # Table 5-18: Filestore Response Status Codes
  CREATE_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "NOT_ALLOWED" => 1,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  DELETE_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "FILE_DOES_NOT_EXIST" => 1,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  RENAME_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "OLD_FILE_DOES_NOT_EXIST" => 1,
    "NEW_FILE_ALREADY_EXISTS" => 2,
    "NOT_ALLOWED" => 3,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  APPEND_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "FILE_1_DOES_NOT_EXIST" => 1,
    "FILE_2_DOES_NOT_EXIST" => 2,
    "NOT_ALLOWED" => 3,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  REPLACE_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "FILE_1_DOES_NOT_EXIST" => 1,
    "FILE_2_DOES_NOT_EXIST" => 2,
    "NOT_ALLOWED" => 3,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  CREATE_DIRECTORY_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "CANNOT_BE_CREATED" => 1,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  REMOVE_DIRECTORY_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "DOES_NOT_EXIST" => 1,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  DENY_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  DENY_DIRECTORY_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  FILESTORE_RESPONSE_STATUS_CODES = {
    "CREATE_FILE" => CREATE_FILE_STATUS_CODES,
    "DELETE_FILE" => DELETE_FILE_STATUS_CODES,
    "RENAME_FILE" => RENAME_FILE_STATUS_CODES,
    "APPEND_FILE" => APPEND_FILE_STATUS_CODES,
    "REPLACE_FILE" => REPLACE_FILE_STATUS_CODES,
    "CREATE_DIRECTORY" => CREATE_DIRECTORY_STATUS_CODES,
    "REMOVE_DIRECTORY" => REMOVE_DIRECTORY_STATUS_CODES,
    "DENY_FILE" => DENY_FILE_STATUS_CODES,
    "DENY_DIRECTORY" => DENY_DIRECTORY_STATUS_CODES
  }

  # Table 5-14: File Data PDU Contents
  RECORD_CONTINUATION_STATES = {
    "NEITHER_START_NOR_END" => 0,
    "START" => 1,
    "END" => 2,
    "START_AND_END" => 3
  }

  # Defined in Section 5.4
  TLV_TYPES = {
    "FILESTORE_REQUEST" => 0,
    "FILESTORE_RESPONSE" => 1,
    "MESSAGE_TO_USER" => 2,
    "FAULT_HANDLER_OVERRIDE" => 4,
    "FLOW_LABEL" => 5,
    "ENTITY_ID" => 6
  }

  # Table 5-19
  HANDLER_CODES = {
    "ISSUE_NOTICE_OF_CANCELATION" => 1,
    "ISSUE_NOTICE_OF_SUSPENSION" => 2,
    "IGNORE_ERROR" => 3,
    "ABONDON_TRANSACTION" => 4
  }

  def initialize(crcs_required:)
    super()
    append_item("VERSION", 3, :UINT)
    item = append_item("TYPE", 1, :UINT)
    item.states = {"FILE_DIRECTIVE" => 0, "FILE_DATA" => 1}
    item = append_item("DIRECTION", 1, :UINT)
    item.states = {"TOWARD_FILE_RECEIVER" => 0, "TOWARD_FILE_SENDER" => 1}
    item = append_item("TRANSMISSION_MODE", 1, :UINT)
    item.states = {"ACKNOWLEDGED" => 0, "UNACKNOWLEDGED" => 1}
    item = append_item("CRC_FLAG", 1, :UINT)
    item.states = {"CRC_NOT_PRESENT" => 0, "CRC_PRESENT" => 1}
    item = append_item("LARGE_FILE_FLAG", 1, :UINT)
    item.states = {"SMALL_FILE" => 0, "LARGE_FILE" => 1}
    item = append_item("PDU_DATA_LENGTH", 16, :UINT)
    item = append_item("SEGMENTATION_CONTROL", 1, :UINT)
    item.states = {"NOT_PRESERVED" => 0, "PRESERVED" => 1}
    item = append_item("ENTITY_ID_LENGTH", 3, :UINT)
    item = append_item("SEGMENT_METADATA_FLAG", 1, :UINT)
    item.states = {"NOT_PRESENT" => 0, "PRESENT" => 1}
    item = append_item("SEQUENCE_NUMBER_LENGTH", 3, :UINT)
    if crcs_required
      item = append_item("VARIABLE_DATA", -16, :BLOCK)
      item = define_item("CRC", -16, 16, :UINT)
    else
      item = append_item("VARIABLE_DATA", 0, :BLOCK)
    end
  end

  def self.decom(pdu_data)
    pdu_hash = {}
    source_entity = CfdpMib.source_entity
    pdu = new(crcs_required: source_entity['crcs_required'])
    pdu.buffer = pdu

    # Static header
    keys = [
      "VERSION",
      "TYPE",
      "DIRECTION",
      "TRANSMISSION_MODE",
      "CRC_FLAG",
      "LARGE_FILE_FLAG",
      "PDU_DATA_LENGTH",
      "SEGMENTATION_CONTROL",
      "ENTITY_ID_LENGTH",
      "SEGMENT_METADATA_FLAG",
      "SEQUENCE_NUMBER_LENGTH",
      "VARIABLE_DATA"
    ]
    keys << "CRC" if source_entity['crcs_required']
    keys.each do |key|
      pdu_hash[key] = pdu.read(key)
    end

    # Variable Header
    s = pdu.define_variable_header
    variable_header = pdu_hash['VARIABLE_DATA'][0..(s.defined_length - 1)]
    s.buffer = variable_header
    variable_header_keys = [
      "SOURCE_ENTITY_ID",
      "SEQUENCE_NUMBER",
      "DESTINATION_ENTITY_ID"
    ]
    keys << "DIRECTIVE_CODE" if pdu_hash['TYPE'] == "FILE_DIRECTIVE"
    variable_header_keys.each do |key|
      pdu_hash[key] = s.read(key)
    end

    variable_data = pdu_hash['VARIABLE_DATA'][(s.defined_length)..-1]

    # PDU Specific Data
    case pdu_hash["DIRECTIVE_CODE"]
    when "EOF"
      decom_eof_pdu_contents(pdu, pdu_hash, variable_data)

    when "FINISHED"
      decom_finished_pdu_contents(pdu, pdu_hash, variable_data)

    when "ACK"
    when "METADATA"

    when "NAK"
    when "PROMPT"
    when "KEEP_ALIVE"
    else # File Data
      decom_file_data_pdu_contents(pdu, pdu_hash, variable_data)

    end

    return pdu_hash
  end

  def self.build_initial_pdu(destination_entity:, file_size:, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
    pdu = self.new(crcs_required: destination_entity['crcs_required'])
    pdu.write("VERSION", 3, destination_entity['protocol_version_number'])
    pdu.write("TYPE", "FILE_DATA")
    pdu.write("DIRECTION", "TOWARD_FILE_RECEIVER")
    if transmission_mode
      pdu.write("TRANSMISSION_MODE", transmission_mode)
    else
      pdu.write("TRANSMISSION_MODE", destination_entity['default_transmission_mode'])
    end
    if destination_entity['crcs_required']
      pdu.write("CRC_FLAG", "CRC_PRESENT")
    else
      pdu.write("CRC_FLAG", "CRC_NOT_PRESENT")
    end
    if file_size >= 4_294_967_296
      pdu.write("LARGE_FILE_FLAG", "LARGE_FILE")
    else
      pdu.write("LARGE_FILE_FLAG", "SMALL_FILE")
    end
    pdu.write("SEGMENTATION_CONTROL", segmentation_control)
    pdu.write("ENTITY_ID_LENGTH", destination_entity['entity_id_length'])
    pdu.write("SEGMENT_METADATA_FLAG", "NOT_PRESENT") # Not implemented
    pdu.write("SEQUENCE_NUMBER_LENGTH", destination_entity['sequence_number_length'])
    return pdu
  end

  def define_variable_header
    id_length = read("ENTITY_ID_LENGTH") + 1
    seq_num_length = read("SEQUENCE_NUMBER_LENGTH") + 1
    type = read("TYPE")
    s = OpenC3::Structure.new(:BIG_ENDIAN)
    s.append_item("SOURCE_ENTITY_ID", id_length * 8, :UINT)
    s.append_item("SEQUENCE_NUMBER", seq_num_length * 8, :UINT, nil, :BIG_ENDIAN, :TRUNCATE)
    s.append_item("DESTINATION_ENTITY_ID", id_length * 8, :UINT)
    if type == "FILE_DIRECTIVE"
      s.append_item("DIRECTIVE_CODE", 8, :UINT)
      s.states = DIRECTIVE_CODES
    end
    return s
  end

  def build_variable_header(source_entity_id:, transaction_seq_num:, destination_entity_id:, directive_code: nil)
    s = define_variable_header()
    s.write("SOURCE_ENTITY_ID", source_entity_id)
    s.write("SEQUENCE_NUMBER", transaction_seq_num)
    s.write("DESTINATION_ENTITY_ID", destination_entity_id)
    s.write("DIRECTIVE_CODE", directive_code) if directive_code
    return s.buffer(false)
  end

  def self.checksum_type_implemented(checksum_type)
    if [0,15].include?(checksum_type)
      return true
    else
      return false
    end
  end

  def self.decom_eof_pdu_contents(pdu, pdu_hash, variable_data)
    s = pdu.define_eof_pdu_contents(variable_data: variable_data)
    s.buffer = variable_data
    pdu_hash["CONDITION_CODE"] = s.read("CONDITION_CODE")
    pdu_hash["FILE_CHECKSUM"] = s.read("FILE_CHECKSUM")
    pdu_hash["FILE_SIZE"] = s.read("FILE_SIZE")
    if pdu_hash['CONDITION_CODE'] != "NO_ERROR"
      pdu_hash["CANCEL_TYPE"] = s.read("CANCEL_TYPE")
      pdu_hash["CANCEL_LENGTH"] = s.read("CANCEL_LENGTH")
      pdu_hash["CANCEL_VALUE"] = s.read("CANCEL_VALUE")
    end
  end

  def self.build_eof_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    file_checksum:,
    condition_code:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    canceling_entity_id: nil)

    pdu = build_initial_pdu(destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "EOF")
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = build_eof_pdu_contents(condition_code: condition_code, file_checksum: file_checksum, file_size: file_size, canceling_entity_id: canceling_entity_id)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_eof_pdu_contents(variable_data: nil, canceling_entity_id: nil)
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    s.append_item("SPARE", 4, :UINT)
    s.append_item("FILE_CHECKSUM", 32, :UINT)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      s.append_item("FILE_SIZE", 32, :UINT)
    else
      s.append_item("FILE_SIZE", 64, :UINT)
    end
    if canceling_entity_id or (variable_data and variable_data.length > s.length)
      entity_id_length = read("ENTITY_ID_LENGTH") + 1
      s.append_item("CANCEL_TYPE", 8, :UINT)  # 0x06
      s.append_item("CANCEL_LENGTH", 8, :UINT) # ENTITY_ID_LENGTH + 1
      s.append_item("CANCEL_VALUE", entity_id_length * 8, :UINT) # canceling_entity_id
    end
    return s
  end

  def build_eof_pdu_contents(condition_code:, file_checksum:, file_size:, canceling_entity_id: nil)
    s = define_eof_pdu(canceling_entity_id: canceling_entity_id)
    if canceling_entity_id
      entity_id_length = read("ENTITY_ID_LENGTH") + 1
      s.write("CANCEL_TYPE", 0x06)
      s.write("CANCEL_LENGTH", entity_id_length)
      s.write("CANCEL_VALUE", canceling_entity_id)
    end
    s.write("CONDITION_CODE", condition_code)
    s.write("SPARE", 0)
    s.write("FILE_CHECKSUM", file_checksum)
    s.write("FILE_SIZE", file_size)
    return s.buffer(false)
  end

  def self.decom_finished_pdu_contents(pdu, pdu_hash, variable_data)
    s = pdu.define_finished_pdu_contents
    s.buffer = variable_data
    pdu_hash["CONDITION_CODE"] = s.read("CONDITION_CODE")
    pdu_hash["DELIVERY_CODE"] = s.read("DELIVERY_CODE")
    pdu_hash["FILE_STATUS"] = s.read("FILE_STATUS")
    variable_data = variable_data[s.defined_length..-1]
    while variable_data.length > 0
      variable_data = decom_tlv(pdu, pdu_hash, variable_data)
    end
  end

  def self.build_finished_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    condition_code:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    delivery_code:,
    file_status:,
    filestore_responses: [],
    fault_location_entity_id: nil)

    pdu = build_initial_pdu(destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "FINISHED")
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = build_finished_pdu_contents(condition_code: condition_code, delivery_code: delivery_code, file_status: file_status, filestore_responses: filestore_responses, fault_location_entity_id: fault_location_entity_id)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def self.decom_tlv(pdu, pdu_hash, variable_data)
    if variable_data.length >= 2 # Need at least 2 bytes
      s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
      s.append_item("TLV_TYPE", 8, :UINT)
      s.states = TLV_TYPES
      s.append_item("TLV_LENGTH", 8, :UINT)
      s.buffer = variable_data
      type = s.read("TLV_TYPE")
      length = s.read("TLV_LENGTH")
      if length > 0
        tlv_data = variable_data[0..(length + 1)]
        variable_data = variable_data[(length + 1)..-1]
        tlv = {}
        tlv["TYPE"] = type
        pdu_hash["TLVS"] ||= []
        pdu_hash["TLVS"] << tlv

        case type
        when "FILESTORE_REQUEST"
          s, s2 = define_filestore_request_tlv()
          s.buffer = tlv_data
          first_file_name_length = s.read("FIRST_FILE_NAME_LENGTH")
          s.buffer = tlv_data[0..(4 + first_file_name_length - 1)]
          tlv_data = tlv_data[(4 + first_file_name_length)..-1]
          tlv["ACTION_CODE"] = s.read("ACTION_CODE")
          tlv["FIRST_FILE_NAME"] = s.read("FIRST_FILE_NAME")
          if tlv_data.length > 0
            s2.buffer = tlv_data
            second_file_name_length = s2.read("SECOND_FILE_NAME_LENGTH")
            s2.buffer = tlv_data[0..(1 + second_file_name_length - 1)]
            tlv["SECOND_FILE_NAME"] = s.read("SECOND_FILE_NAME")
          end

        when "FILESTORE_RESPONSE"
          s, s2, s3, status_code_item = define_filestore_response_tlv()
          s.buffer = tlv_data
          first_file_name_length = s.read("FIRST_FILE_NAME_LENGTH")
          s.buffer = tlv_data[0..(4 + first_file_name_length - 1)]
          tlv_data = tlv_data[(4 + first_file_name_length)..-1]
          tlv["ACTION_CODE"] = s.read("ACTION_CODE")
          add_status_code_states(action_code: tlv["ACTION_CODE"], status_code_item: status_code_item)
          tlv["STATUS_CODE"] = s.read("STATUS_CODE")
          tlv["FIRST_FILE_NAME"] = s.read("FIRST_FILE_NAME")
          if tlv_data.length > 0
            s2.buffer = tlv_data
            second_file_name_length = s2.read("SECOND_FILE_NAME_LENGTH")
            s2.buffer = tlv_data[0..(1 + second_file_name_length - 1)]
            tlv_data = tlv_data[(1 + second_file_name_length)..-1]
            tlv["SECOND_FILE_NAME"] = s.read("SECOND_FILE_NAME")
            if tlv_data.length > 0
              s3.buffer = tlv_data
              filestore_message_length = s3.read("FILESTORE_MESSAGE_LENGTH")
              s3.buffer = tlv_data[0..(1 + filestore_message_length - 1)]
              tlv_data = tlv_data[(1 + filestore_message_length)..-1]
              tlv["FILESTORE_MESSAGE"] = s.read("FILESTORE_MESSAGE")
            end
          end

        when "MESSAGE_TO_USER"
          s = define_message_to_user_tlv()
          s.buffer = tlv_data
          tlv["MESSAGE_TO_USER"] = s.read("MESSAGE_TO_USER")

        when "FAULT_HANDLER_OVERRIDE"
          s = define_fault_handler_override_tlv()
          s.buffer = tlv_data
          tlv["CONDITION_CODE"] = s.read("CONDITION_CODE")
          tlv["HANDLER_CODE"] = s.read("HANDLER_CODE")

        when "FLOW_LABEL"
          s = define_flow_label_tlv()
          s.buffer = tlv_data
          tlv["FLOW_LABEL"] = s.read("FLOW_LABEL")

        when "ENTITY_ID"
          s = define_entity_id_tlv()
          s.buffer = tlv_data
          entity_id_string = s.read("ENTITY_ID")
          s2 = OpenC3::Structure.new(:BIG_ENDIAN)
          s2.append_item("ENTITY_ID", entity_id_string.length * 8, :UINT)
          s2.buffer = entity_id_string
          tlv["ENTITY_ID"] = s2.read("ENTITY_ID")

        end
      else
        variable_data = variable_data[2..-1]
      end
      return variable_data
    else
      return ""
    end
  end

  # Table 5-15
  def self.define_filestore_request_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("TLV_TYPE", 8, :UINT) # 0x01
    s.append_item("TLV_LENGTH", 8, :UINT)
    item = s.append_item("ACTION_CODE", 4, :UINT)
    item.states = ACTION_CODES
    s.append_item("SPARE", 4, :UINT)
    s.append_item("FIRST_FILE_NAME_LENGTH", 8, :UINT)
    s.append_item("FIRST_FILE_NAME", 0, :BLOCK)

    s2 = OpenC3::Structure.new(:BIG_ENDIAN)
    s2.append_item("SECOND_FILE_NAME_LENGTH", 8, :UINT)
    s2.append_item("SECOND_FILE_NAME", 0, :BLOCK)

    return s, s2
  end

  # Table 5-17
  def self.define_filestore_response_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("TLV_TYPE", 8, :UINT) # 0x01
    s.append_item("TLV_LENGTH", 8, :UINT)
    item = s.append_item("ACTION_CODE", 4, :UINT)
    item.states = ACTION_CODES
    status_code_item = s.append_item("STATUS_CODE", 4, :UINT)
    s.append_item("FIRST_FILE_NAME_LENGTH", 8, :UINT)
    s.append_item("FIRST_FILE_NAME", 0, :BLOCK)

    s2 = OpenC3::Structure.new(:BIG_ENDIAN)
    s2.append_item("SECOND_FILE_NAME_LENGTH", 8, :UINT)
    s2.append_item("SECOND_FILE_NAME", 0, :BLOCK)

    s3 = OpenC3::Structure.new(:BIG_ENDIAN)
    s3.append_item("FILESTORE_MESSAGE_LENGTH", 8, :UINT)
    s3.append_item("FILESTORE_MESSAGE", 0, :BLOCK)

    return s, s2, s3, status_code_item
  end

  # Table 5-18
  def self.add_status_code_states(action_code:, status_code_item:)
    if String === action_code
      status_code_item.states = FILESTORE_RESPONSE_STATUS_CODES[action_code]
    else
      status_code_item.states = FILESTORE_RESPONSE_STATUS_CODES[ACTION_CODES.key(action_code)]
    end
  end

  # Section 5.4.3
  def self.define_message_to_user_tlv
    # Todo: Improve for Proxy support
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("TLV_TYPE", 8, :UINT) # 0x02
    s.append_item("TLV_LENGTH", 8, :UINT)
    s.append_item("MESSAGE_TO_USER", 0, :BLOCK)
    return s
  end

  # Table 5-19
  def self.define_fault_handler_override_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("TLV_TYPE", 8, :UINT) # 0x04
    s.append_item("TLV_LENGTH", 8, :UINT)
    s.append_item("CONDITION_CODE", 4, :UINT)
    s.states = CONDITION_CODES
    s.append_item("HANDLER_CODE", 4, :UINT)
    s.states = HANDLER_CODES
    return s
  end

  # Section 5.4.5
  def self.define_flow_label_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("TLV_TYPE", 8, :UINT) # 0x05
    s.append_item("TLV_LENGTH", 8, :UINT)
    s.append_item("FLOW_LABEL", 0, :BLOCK)
    return s
  end

  # See Section 5.4.6
  def self.define_entity_id_tlv
    s = OpenC3::Structure.new(:BIG_ENDIAN)
    s.append_item("TLV_TYPE", 8, :UINT) # 0x06
    s.append_item("TLV_LENGTH", 8, :UINT)
    s.append_item("ENTITY_ID", 0, :BLOCK)
    return s
  end

  def define_finished_pdu_contents
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    s.append_item("SPARE", 1, :UINT)
    item = s.append_item("DELIVERY_CODE", 1, :UINT)
    item.states = DELIVERY_CODES
    item = s.append_item("FILE_STATUS", 2, :UINT)
    item.states = FILE_STATUS_CODES
    return s
  end

  def build_finished_pdu_contents(condition_code:, delivery_code:, file_status:, filestore_responses: [], fault_location_entity_id: nil)
    structures = []
    s = define_finished_pdu_contents()
    s.write("CONDITION_CODE", condition_code)
    s.write("SPARE", 0)
    s.write("DELIVERY_CODE", delivery_code)
    s.write("FILE_STATUS", file_status)
    structures << s

    filestore_responses.each do |filestore_response|
      action_code = filestore_response['action_code']
      status_code = filestore_response['status_code']
      first_file_name = filestore_response['first_file_name']
      second_file_name = filestore_response['second_file_name']
      filestore_message = filestore_response['filestore_message']
      s, s2, s3, status_code_item = define_filestore_response_tlv()
      add_status_code_states(action_code: action_code, status_code_item: status_code_item)
      s.write("TLV_TYPE", 0x01)
      s.write("TLV_LENGTH", 4 + first_file_name.to_s.length + second_file_name.to_s.length + filestore_message.to_s.length)
      s.write("ACTION_CODE", action_code)
      s.write("STATUS_CODE", status_code)
      s.write("FIRST_FILE_NAME_LENGTH", first_file_name.to_s.length)
      s.write("FIRST_FILE_NAME", first_file_name.to_s) if first_file_name.to_s.length > 0
      s2.write("SECOND_FILE_NAME_LENGTH", second_file_name.to_s.length)
      s2.write("SECOND_FILE_NAME", second_file_name.to_s) if second_file_name.to_s.length > 0
      s3.write("FILESTORE_MESSAGE_LENGTH", filestore_message.to_s.length)
      s3.write("FILESTORE_MESSAGE", filestore_message.to_s) if filestore_message.to_s.length > 0
      structures << s
      structures << s2
      structures << s3
    end
    if fault_location_entity_id
      s = define_entity_id_tlv()
      entity_id_length = read("ENTITY_ID_LENGTH") + 1
      s.write("FAULT_LOCATION_TYPE", 0x06)
      s.write("FAULT_LOCATION_LENGTH", entity_id_length)
      s.write("FAULT_LOCATION_VALUE", fault_location_entity_id)
      structures << s
    end
    result = ''
    structures.each do |s|
      result << s.buffer(false)
    end
    return result
  end

  def build_ack_pdu_contents(ack_directive_code:, condition_code:, transaction_status:)
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("ACK_DIRECTIVE_CODE", 4, :UINT)
    item.states = DIRECTIVE_CODES
    s.append_item("ACK_DIRECTIVE_SUBTYPE", 4, :UINT)
    item = s.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    s.append_item("SPARE", 2, :UINT)
    item = s.append_item("TRANSACTION_STATUS", 2, :UINT)
    item.states = TRANSACTION_STATUS_CODES

    s.write("ACK_DIRECTIVE_CODE", ack_directive_code)
    if s.read("ACK_DIRECTIVE_CODE") == "FINISHED"
      s.write("ACK_DIRECTIVE_SUBTYPE", 1)
    else
      s.write("ACK_DIRECTIVE_SUBTYPE", 0)
    end
    s.write("CONDITION_CODE", condition_code)
    s.write("SPARE", 0)
    s.write("TRANSACTION_STATUS", transaction_status)
    return s.buffer(false)
  end

  def self.build_metadata_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    closure_requested:,
    file_size:,
    source_file_name: nil,
    destination_file_name: nil,
    options: [],
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil)

    pdu = build_initial_pdu(destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "METADATA")
    pdu_header_part_2_length = pdu_header.length
    if checksum_type_implemented(destination_entity['default_checksum_type'])
      checksum_type = destination_entity['default_checksum_type']
    else
      checksum_type = 0
    end
    pdu_contents = build_meta_data_pdu_contents(closure_requested: closure_requested, checksum_type: checksum_type, file_size: file_size, source_file_name: source_file_name, destination_file_name: destination_file_name, options: options)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_metadata_pdu_contents(options: [])
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("RESERVED", 1, :UINT)
    item = s.append_item("CLOSURE_REQUESTED", 1, :UINT)
    item.states = {"CLOSURE_NOT_REQUESTED" => 0, "CLOSURE_REQUESTED" => 1}
    s.append_item("RESERVED2", 2, :UINT)
    s.append_item("CHECKSUM_TYPE", 4, :UINT)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      s.append_item("FILE_SIZE", 32, :UINT)
    else
      s.append_item("FILE_SIZE", 64, :UINT)
    end
    s.append_item("SOURCE_FILE_NAME_LENGTH", 8, :UINT)
    s.append_item("SOURCE_FILE_NAME", 0, :BLOCK)

    s2 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s2.append_item("DESTINATION_FILE_NAME_LENGTH", 8, :UINT)
    s2.append_item("DESTINATION_FILE_NAME", 0, :BLOCK)

    options.each do |option|
      # TODO: Handle Options
    end
    return s, s2
  end

  def build_metadata_pdu_contents(closure_requested:, checksum_type:, file_size:, source_file_name: nil, destination_file_name: nil, options: [])
    s, s2 = define_metadata_pdu_contents(options: options)
    s.write("RESERVED", 0)
    s.write("CLOSURE_REQUESTED", closure_requested)
    s.write("RESERVED2", 0)
    s.write("CHECKSUM_TYPE", checksum_type)
    s.write("FILE_SIZE", file_size)
    s.write("SOURCE_FILE_NAME_LENGTH", source_file_name.to_s.length)
    s.write("SOURCE_FILE_NAME", source_file_name.to_s) if source_file_name.to_s.length > 0
    s2.write("DESTINATION_FILE_NAME_LENGTH", destination_file_name.to_s.length)
    s2.write("DESTINATION_FILE_NAME", destination_file_name.to_s) if destination_file_name.to_s.length > 0
    options.each do |option|
      # TODO: Handle Options
    end

    return s.buffer(false) + s2.buffer(false)
  end

  def build_nak_pdu_contents(start_of_scope:, end_of_scope:, segment_requests: [])
    s = OpenC3::Structure.new(:BIG_ENDIAN)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      item_size = 32
    else
      item_size = 64
    end

    s.append_item("START_OF_SCOPE", item_size, :UINT)
    s.append_item("END_OF_SCOPE", item_size, :UINT)
    segment_requests.each_with_index do |segment_request, index|
      s.append_item("SR#{index}_START_OFFSET", item_size, :UINT)
      s.append_item("SR#{index}_END_OFFSET", item_size, :UINT)
    end

    s.write("START_OF_SCOPE", start_of_scope)
    s.write("END_OF_SCOPE", end_of_scope)
    segment_requests.each_with_index do |segment_request, index|
      s.write("SR#{index}_START_OFFSET", segment_request[0])
      s.write("SR#{index}_END_OFFSET", segment_request[1])
    end

    return s.buffer(false)
  end

  def build_prompt_pdu_contents(response_required:)
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("RESPONSE_REQUIRED", 1, :UINT)
    item.states = {"NAK" => 0, "KEEP_ALIVE" => 1}
    s.append_item("SPARE", 7, :UINT)
    s.write("RESPONSE_REQUIRED", response_required)
    s.write("SPARE", 0)
    return s.buffer(false)
  end

  def build_keep_alive_pdu_contents(progress:)
    s = OpenC3::Structure.new(:BIG_ENDIAN)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      item_size = 32
    else
      item_size = 64
    end
    s.append_item("PROGRESS", item_size, :UINT)
    s.write("PROGRESS", progress)
    return s.buffer(false)
  end

  def self.decom_file_data_pdu_contents(pdu, pdu_hash, variable_data)
    s, s2 = pdu.define_file_data_pdu_contents
    if s
      s.buffer = variable_data
      pdu_hash["RECORD_CONTINUATION_STATE"] = s.read("RECORD_CONTINUATION_STATE")
      pdu_hash["SEGMENT_METADATA_LENGTH"] = s.read("SEGMENT_METADATA_LENGTH")
      s.buffer = variable_data[0..pdu_hash["SEGMENT_METADATA_LENGTH"]]
      pdu_hash["SEGMENT_METADATA"] = s.read("SEGMENT_METADATA")
      variable_data = variable_data[(s.length)..-1]
    end
    s2.buffer = variable_data
    pdu_hash['OFFSET'] = s2.read("OFFSET")
    pdu_hash['FILE_DATA'] = s2.read("FILE_DATA")
  end

  def self.build_file_data_pdu(
    offset:,
    file_data:,
    file_size:,
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil)

    pdu = build_initial_pdu(destination_entity: destination_entity, file_size: file_size, segmentation_control: segmentation_control, transmission_mode: transmission_mode)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'])
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = build_file_data_pdu_contents(offset: offset, file_data: file_data)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_file_data_pdu_contents
    smf = read("SEGMENT_METADATA_FLAG")

    s = nil
    if smf == "PRESENT"
      s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
      item = s.append_item("RECORD_CONTINUATION_STATE", 2, :UINT)
      item.states = RECORD_CONTINUATION_STATES
      s.append_item("SEGMENT_METADATA_LENGTH", 6, :UINT)
      s.append_item("SEGMENT_METADATA", 0, :BLOCK)
    end

    s2 = OpenC3::Structure.new(:BIG_ENDIAN)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      item_size = 32
    else
      item_size = 64
    end
    s2.append_item("OFFSET", item_size, :UINT)
    s2.append_item("FILE_DATA", 0, :BLOCK)

    return s, s2
  end

  def build_file_data_pdu_contents(offset:, file_data:, record_continuation_state: nil, segment_metadata: nil)
    s, s2 = define_file_data_pdu_contents()
    s2.write("OFFSET", offset)
    s2.write("FILE_DATA", file_data.to_s)

    if s
      s.write("RECORD_CONTINUATION_STATE", record_continuation_state)
      s.write("SEGMENT_METADATA_LENGTH", segment_metadata.to_s.length)
      s.write("SEGMENT_METADATA", segment_metadata.to_s)
      return s.buffer(false) + s2.buffer(false)
    else
      return s2.buffer(false)
    end
  end
end

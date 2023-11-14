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

require 'openc3/packets/packet'
require 'openc3/utilities/crc'

require 'cfdp_pdu/cfdp_pdu_enum'
require 'cfdp_pdu/cfdp_pdu_tlv'
require 'cfdp_pdu/cfdp_pdu_eof'
require 'cfdp_pdu/cfdp_pdu_finished'
require 'cfdp_pdu/cfdp_pdu_ack'
require 'cfdp_pdu/cfdp_pdu_metadata'
require 'cfdp_pdu/cfdp_pdu_nak'
require 'cfdp_pdu/cfdp_pdu_prompt'
require 'cfdp_pdu/cfdp_pdu_keep_alive'
require 'cfdp_pdu/cfdp_pdu_file_data'
require 'cfdp_pdu/cfdp_pdu_user_ops'

class CfdpPdu < OpenC3::Packet
  DIRECTIVE_CODE_BYTE_SIZE = 1
  CRC_BYTE_SIZE = 2

  def initialize(crcs_required:)
    super()
    append_item("VERSION", 3, :UINT)
    item = append_item("TYPE", 1, :UINT)
    item.states = {"FILE_DIRECTIVE" => 0, "FILE_DATA" => 1}
    item = append_item("DIRECTION", 1, :UINT)
    item.states = {"TOWARD_FILE_RECEIVER" => 0, "TOWARD_FILE_SENDER" => 1}
    item = append_item("TRANSMISSION_MODE", 1, :UINT)
    item.states = TRANSMISSION_MODES
    item = append_item("CRC_FLAG", 1, :UINT)
    item.states = {"CRC_NOT_PRESENT" => 0, "CRC_PRESENT" => 1}
    item = append_item("LARGE_FILE_FLAG", 1, :UINT)
    item.states = {"SMALL_FILE" => 0, "LARGE_FILE" => 1}
    item = append_item("PDU_DATA_LENGTH", 16, :UINT)
    item = append_item("SEGMENTATION_CONTROL", 1, :UINT)
    item.states = SEGMENTATION_MODES
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
    crcs_required = source_entity['crcs_required']
    pdu = new(crcs_required: crcs_required)
    pdu.buffer = pdu_data

    # Handle CRC
    pdu_hash["CRC_FLAG"] = pdu.read("CRC_FLAG")
    if pdu_hash["CRC_FLAG"] == "CRC_PRESENT"
      unless crcs_required
        # Recreate with CRC
        pdu = new(crcs_required: true)
        pdu.buffer = pdu_data
      end
      pdu_hash["CRC"] = pdu.read("CRC")
      crc16 = OpenC3::Crc16.new
      calculated = crc16.calc(pdu.buffer(false)[0..-3])
      if pdu_hash["CRC"] != calculated
        raise "PDU with invalid CRC received: Received: #{sprintf("0x%04X", pdu_hash["CRC"])}, Calculated: #{sprintf("0x%04X", calculated)}"
      end
    elsif crcs_required
      raise "PDU without required CRC received"
    end

    # Static header
    keys = [
      "VERSION",
      "TYPE",
      "DIRECTION",
      "TRANSMISSION_MODE",
      "LARGE_FILE_FLAG",
      "PDU_DATA_LENGTH",
      "SEGMENTATION_CONTROL",
      "ENTITY_ID_LENGTH",
      "SEGMENT_METADATA_FLAG",
      "SEQUENCE_NUMBER_LENGTH",
      "VARIABLE_DATA"
    ]
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
    variable_header_keys << "DIRECTIVE_CODE" if pdu_hash['TYPE'] == "FILE_DIRECTIVE"
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
      decom_ack_pdu_contents(pdu, pdu_hash, variable_data)
    when "METADATA"
      decom_metadata_pdu_contents(pdu, pdu_hash, variable_data)
    when "NAK"
      decom_nak_pdu_contents(pdu, pdu_hash, variable_data)
    when "PROMPT"
      decom_prompt_pdu_contents(pdu, pdu_hash, variable_data)
    when "KEEP_ALIVE"
      decom_keep_alive_pdu_contents(pdu, pdu_hash, variable_data)
    else # File Data
      decom_file_data_pdu_contents(pdu, pdu_hash, variable_data)
    end

    return pdu_hash
  end

  def self.build_initial_pdu(type:, destination_entity:, file_size:, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
    pdu = self.new(crcs_required: destination_entity['crcs_required'])
    pdu.write("VERSION", destination_entity['protocol_version_number'])
    pdu.write("TYPE", type)
    pdu.write("DIRECTION", "TOWARD_FILE_RECEIVER")
    if transmission_mode
      transmission_mode = transmission_mode.upcase
      pdu.write("TRANSMISSION_MODE", transmission_mode)
    else
      pdu.write("TRANSMISSION_MODE", destination_entity['default_transmission_mode'].upcase)
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
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("SOURCE_ENTITY_ID", id_length * 8, :UINT)
    s.append_item("SEQUENCE_NUMBER", seq_num_length * 8, :UINT, nil, :BIG_ENDIAN, :TRUNCATE)
    s.append_item("DESTINATION_ENTITY_ID", id_length * 8, :UINT)
    if type == "FILE_DIRECTIVE"
      item = s.append_item("DIRECTIVE_CODE", 8, :UINT)
      item.states = DIRECTIVE_CODES
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
    if [0,1,2,3,15].include?(checksum_type)
      return true
    else
      return false
    end
  end
end

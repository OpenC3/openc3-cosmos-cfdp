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

class CfdpPdu < OpenC3::Packet
  def self.decom_eof_pdu_contents(pdu, pdu_hash, variable_data)
    s = pdu.define_eof_pdu_contents()
    s.buffer = variable_data[0..(s.defined_length - 1)]
    pdu_hash["CONDITION_CODE"] = s.read("CONDITION_CODE")
    pdu_hash["FILE_CHECKSUM"] = s.read("FILE_CHECKSUM")
    pdu_hash["FILE_SIZE"] = s.read("FILE_SIZE")
    if pdu_hash['CONDITION_CODE'] != "NO_ERROR"
      variable_data = variable_data[s.defined_length..-1]
      decom_tlv(pdu, pdu_hash, variable_data)
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

    pdu = build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "EOF")
    pdu_header_part_2_length = pdu_header.length - 1 # Minus 1 = Directive code is part of data per 5.2.1.1
    pdu_contents = pdu.build_eof_pdu_contents(condition_code: condition_code, file_checksum: file_checksum, file_size: file_size, canceling_entity_id: canceling_entity_id)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_eof_pdu_contents
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
    return s
  end

  def build_eof_pdu_contents(condition_code:, file_checksum:, file_size:, canceling_entity_id: nil)
    s = define_eof_pdu_contents()
    s.write("CONDITION_CODE", condition_code)
    s.write("SPARE", 0)
    s.write("FILE_CHECKSUM", file_checksum)
    s.write("FILE_SIZE", file_size)
    if canceling_entity_id
      entity_id_length = read("ENTITY_ID_LENGTH") + 1
      s2 = CfdpPdu.define_entity_id_tlv(entity_id_length: entity_id_length)
      s2.write("TLV_TYPE", 0x06)
      s2.write("TLV_LENGTH", entity_id_length)
      s2.write("ENTITY_ID", canceling_entity_id)
      return s.buffer(false) + s2.buffer(false)
    else
      return s.buffer(false)
    end
  end
end

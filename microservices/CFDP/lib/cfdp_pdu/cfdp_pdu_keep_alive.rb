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
  def self.decom_keep_alive_pdu_contents(pdu, pdu_hash, variable_data)
    s = pdu.define_keep_alive_pdu_contents
    s.buffer = variable_data
    pdu_hash["PROGRESS"] = s.read("PROGRESS")
  end

  def self.build_keep_alive_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    progress:)

    pdu = build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu.write("DIRECTION", "TOWARD_FILE_SENDER")
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data - Includes CRC if present
    pdu_header_part_1_length -= CRC_BYTE_SIZE if destination_entity['crcs_required'] # PDU_DATA_LENGTH field should contain CRC length
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "KEEP_ALIVE")
    pdu_header_part_2_length = pdu_header.length - DIRECTIVE_CODE_BYTE_SIZE # Minus 1 = Directive code is part of data per 5.2.1.1
    pdu_contents = pdu.build_keep_alive_pdu_contents(progress: progress)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_keep_alive_pdu_contents
    s = OpenC3::Structure.new(:BIG_ENDIAN)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      item_size = 32
    else
      item_size = 64
    end
    s.append_item("PROGRESS", item_size, :UINT)
    return s
  end

  def build_keep_alive_pdu_contents(progress:)
    s = define_keep_alive_pdu_contents()
    s.write("PROGRESS", progress)
    return s.buffer(false)
  end
end

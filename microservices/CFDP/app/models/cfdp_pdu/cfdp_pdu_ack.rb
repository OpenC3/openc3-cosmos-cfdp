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
  def self.decom_ack_pdu_contents(pdu, pdu_hash, variable_data)
    s = pdu.define_ack_pdu_contents
    s.buffer = variable_data
    pdu_hash["ACK_DIRECTIVE_CODE"] = s.read("ACK_DIRECTIVE_CODE")
    pdu_hash["ACK_DIRECTIVE_SUBTYPE"] = s.read("ACK_DIRECTIVE_SUBTYPE")
    pdu_hash["CONDITION_CODE"] = s.read("CONDITION_CODE")
    pdu_hash["TRANSACTION_STATUS"] = s.read("TRANSACTION_STATUS")
  end

  def self.build_ack_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    condition_code:,
    ack_directive_code:,
    transaction_status:)

    pdu = build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: 0, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "ACK")
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = pdu.build_ack_pdu_contents(ack_directive_code: ack_directive_code, condition_code: condition_code, transaction_status: transaction_status)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_ack_pdu_contents
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("ACK_DIRECTIVE_CODE", 4, :UINT)
    item.states = DIRECTIVE_CODES
    s.append_item("ACK_DIRECTIVE_SUBTYPE", 4, :UINT)
    item = s.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    s.append_item("SPARE", 2, :UINT)
    item = s.append_item("TRANSACTION_STATUS", 2, :UINT)
    item.states = TRANSACTION_STATUS_CODES
    return s
  end

  def build_ack_pdu_contents(ack_directive_code:, condition_code:, transaction_status:)
    s = define_ack_pdu_contents()
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
end

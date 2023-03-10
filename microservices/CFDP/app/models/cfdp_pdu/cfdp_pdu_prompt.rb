# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpPdu < OpenC3::Packet
  def self.decom_prompt_pdu_contents(pdu, pdu_hash, variable_data)
    s = pdu.define_prompt_pdu_contents
    s.buffer = variable_data
    pdu_hash["RESPONSE_REQUIRED"] = s.read("RESPONSE_REQUIRED")
  end

  def self.build_prompt_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    response_required:)

    pdu = build_initial_pdu(destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "PROMPT")
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = pdu.build_prompt_pdu_contents(response_required: response_required)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_prompt_pdu_contents
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("RESPONSE_REQUIRED", 1, :UINT)
    item.states = {"NAK" => 0, "KEEP_ALIVE" => 1}
    s.append_item("SPARE", 7, :UINT)
    return s
  end

  def build_prompt_pdu_contents(response_required:)
    s = define_prompt_pdu_contents()
    s.write("RESPONSE_REQUIRED", response_required)
    s.write("SPARE", 0)
    return s.buffer(false)
  end
end

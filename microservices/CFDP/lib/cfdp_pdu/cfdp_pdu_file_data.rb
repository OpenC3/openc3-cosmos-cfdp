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
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    offset:,
    file_data:,
    record_continuation_state: nil,
    segment_metadata: nil)

    pdu = build_initial_pdu(type: "FILE_DATA", destination_entity: destination_entity, file_size: file_size, segmentation_control: segmentation_control, transmission_mode: transmission_mode)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data - Includes CRC if present
    pdu_header_part_1_length -= CRC_BYTE_SIZE if destination_entity['crcs_required'] # PDU_DATA_LENGTH field should contain CRC length
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'])
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = pdu.build_file_data_pdu_contents(offset: offset, file_data: file_data, record_continuation_state: record_continuation_state, segment_metadata: segment_metadata)
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

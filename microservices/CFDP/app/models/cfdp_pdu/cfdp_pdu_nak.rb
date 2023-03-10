# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpPdu < OpenC3::Packet
  def self.decom_nak_pdu_contents(pdu, pdu_hash, variable_data)
    s, s2 = pdu.define_nak_pdu_contents()
    s.buffer = variable_data
    pdu_hash["START_OF_SCOPE"] = s.read("START_OF_SCOPE")
    pdu_hash["END_OF_SCOPE"] = s.read("END_OF_SCOPE")
    pdu_hash["SEGMENT_REQUESTS"] = []
    variable_data = variable_data[s.defined_length..-1]
    while variable_data and variable_data.length > 0
      s2.buffer = variable_data
      pdu_hash["SEGMENT_REQUESTS"] << {}
      pdu_hash["SEGMENT_REQUESTS"][-1]["START_OFFSET"] = s2.read("START_OFFSET")
      pdu_hash["SEGMENT_REQUESTS"][-1]["END_OFFSET"] = s2.read("END_OFFSET")
      variable_data = variable_data[s2.defined_length..-1]
    end
  end

  def self.build_nak_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    start_of_scope:,
    end_of_scope:,
    segment_requests: [])

    pdu = build_initial_pdu(destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "PROMPT")
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = pdu.build_nak_pdu_contents(start_of_scope: start_of_scope, end_of_scope: end_of_scope, segment_requests: segment_requests)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_nak_pdu_contents
    s = OpenC3::Structure.new(:BIG_ENDIAN)
    large_file = read("LARGE_FILE_FLAG")
    if large_file == "SMALL_FILE"
      item_size = 32
    else
      item_size = 64
    end

    s.append_item("START_OF_SCOPE", item_size, :UINT)
    s.append_item("END_OF_SCOPE", item_size, :UINT)

    s2 = OpenC3::Structure.new(:BIG_ENDIAN)
    s2.append_item("START_OFFSET", item_size, :UINT)
    s2.append_item("END_OFFSET", item_size, :UINT)
    return s, s2
  end

  def build_nak_pdu_contents(start_of_scope:, end_of_scope:, segment_requests: [])
    s, s2 = define_nak_pdu_contents()
    result = ''
    s.write("START_OF_SCOPE", start_of_scope)
    s.write("END_OF_SCOPE", end_of_scope)
    result << s.buffer(false)
    segment_requests.each do |segment_request|
      s2.write("START_OFFSET", segment_request[0])
      s2.write("END_OFFSET", segment_request[1])
      result << s2.buffer(false)
    end
    return result
  end
end

# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpPdu < OpenC3::Packet
  def self.decom_metadata_pdu_contents(pdu, pdu_hash, variable_data)
    s, s2 = pdu.define_metadata_pdu_contents
    s.buffer = variable_data
    pdu_hash["CLOSURE_REQUESTED"] = s.read("CLOSURE_REQUESTED")
    pdu_hash["CHECKSUM_TYPE"] = s.read("CHECKSUM_TYPE")
    pdu_hash["FILE_SIZE"] = s.read("FILE_SIZE")
    source_file_name_length = s.read("SOURCE_FILE_NAME_LENGTH")
    s.buffer = variable_data[0..(s.defined_length + source_file_name_length - 1)]
    pdu_hash["SOURCE_FILE_NAME"] = s.read("SOURCE_FILE_NAME")
    variable_data = variable_data[(s.defined_length + source_file_name_length)..-1]
    if variable_data.length > 0
      s2.buffer = variable_data
      destination_file_name_length = s2.read("DESTINATION_FILE_NAME_LENGTH")
      s2.buffer = variable_data[0..(s2.defined_length + destination_file_name_length - 1)]
      pdu_hash["DESTINATION_FILE_NAME"] = s2.read("DESTINATION_FILE_NAME")
      variable_data = variable_data[(s2.defined_length + destination_file_name_length)..-1]
    end
    while variable_data.length > 0
      variable_data = decom_tlv(pdu, pdu_hash, variable_data)
    end
  end

  def self.build_metadata_pdu(
    source_entity:,
    transaction_seq_num:,
    destination_entity:,
    file_size:,
    segmentation_control: "NOT_PRESERVED",
    transmission_mode: nil,
    source_file_name: nil,
    destination_file_name: nil,
    closure_requested:,
    options: [])

    pdu = build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "METADATA")
    pdu_header_part_2_length = pdu_header.length
    if checksum_type_implemented(destination_entity['default_checksum_type'])
      checksum_type = destination_entity['default_checksum_type']
    else
      checksum_type = 0
    end
    pdu_contents = pdu.build_metadata_pdu_contents(source_entity: source_entity, closure_requested: closure_requested, checksum_type: checksum_type, file_size: file_size, source_file_name: source_file_name, destination_file_name: destination_file_name, options: options)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
  end

  def define_metadata_pdu_contents
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("RESERVED", 1, :UINT)
    item = s.append_item("CLOSURE_REQUESTED", 1, :UINT)
    item.states = CLOSURE_MODES
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

    return s, s2
  end

  def build_metadata_pdu_contents(source_entity:, closure_requested:, checksum_type:, file_size:, source_file_name: nil, destination_file_name: nil, options: [])
    s, s2 = define_metadata_pdu_contents()
    s.write("RESERVED", 0)
    if closure_requested
      s.write("CLOSURE_REQUESTED", closure_requested)
    else
      s.write("CLOSURE_REQUESTED", source_entity['transaction_closure_requested'])
    end
    s.write("RESERVED2", 0)
    s.write("CHECKSUM_TYPE", checksum_type)
    s.write("FILE_SIZE", file_size)
    s.write("SOURCE_FILE_NAME_LENGTH", source_file_name.to_s.length)
    s.write("SOURCE_FILE_NAME", source_file_name.to_s) if source_file_name.to_s.length > 0
    s2.write("DESTINATION_FILE_NAME_LENGTH", destination_file_name.to_s.length)
    s2.write("DESTINATION_FILE_NAME", destination_file_name.to_s) if destination_file_name.to_s.length > 0

    result = s.buffer(false) + s2.buffer(false)
    options.each do |option|
      result << self.class.build_tlv(option)
    end
    return result
  end
end

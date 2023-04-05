# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpPdu < OpenC3::Packet
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

    pdu = build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, transmission_mode: transmission_mode, file_size: file_size, segmentation_control: segmentation_control)
    pdu_header_part_1_length = pdu.length # Measured here before writing variable data
    pdu_header = pdu.build_variable_header(source_entity_id: source_entity['id'], transaction_seq_num: transaction_seq_num, destination_entity_id: destination_entity['id'], directive_code: "FINISHED")
    pdu_header_part_2_length = pdu_header.length
    pdu_contents = pdu.build_finished_pdu_contents(condition_code: condition_code, delivery_code: delivery_code, file_status: file_status, filestore_responses: filestore_responses, fault_location_entity_id: fault_location_entity_id)
    pdu.write("VARIABLE_DATA", pdu_header + pdu_contents)
    pdu.write("PDU_DATA_LENGTH", pdu.length - pdu_header_part_1_length - pdu_header_part_2_length)
    if destination_entity['crcs_required']
      crc16 = OpenC3::Crc16.new
      pdu.write("CRC", crc16.calc(pdu.buffer(false)[0..-3]))
    end
    return pdu.buffer(false)
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
      action_code = filestore_response['ACTION_CODE']
      status_code = filestore_response['STATUS_CODE']
      first_file_name = filestore_response['FIRST_FILE_NAME']
      second_file_name = filestore_response['SECOND_FILE_NAME']
      filestore_message = filestore_response['FILESTORE_MESSAGE']
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
      entity_id_length = read("ENTITY_ID_LENGTH") + 1
      s = CfdpPdu.define_entity_id_tlv(entity_id_length: entity_id_length)
      s.write("TLV_TYPE", 0x06)
      s.write("TLV_LENGTH", entity_id_length)
      s.write("ENTITY_ID", fault_location_entity_id)
      structures << s
    end
    result = ''
    structures.each do |s|
      result << s.buffer(false)
    end
    return result
  end
end

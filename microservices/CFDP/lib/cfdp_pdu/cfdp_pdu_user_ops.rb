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

  # Table 5-2
  def define_length_value
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("LENGTH", 8, :UINT)
    s.append_item("VALUE", 0, :BLOCK)
    return s
  end

  # Table 6-1
  def define_reserved_cfdp_message_header
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("MSG_ID", 32, :STRING)
    item = s.append_item("MSG_TYPE", 8, :UINT)
    item.states = USER_MESSAGE_TYPES
    return s
  end

  # Table 6-2
  def define_originating_transaction_id_message_fixed_header
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("RESERVED1", 1, :UINT)
    s.append_item("ENTITY_ID_LENGTH", 3, :UINT)
    s.append_item("RESERVED2", 1, :UINT)
    s.append_item("SEQUENCE_NUMBER_LENGTH", 3, :UINT)
    return s
  end

  def define_originating_transaction_id_message_variable_data(id_length:, seq_num_length:)
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("SOURCE_ENTITY_ID", id_length * 8, :UINT)
    s.append_item("SEQUENCE_NUMBER", seq_num_length * 8, :UINT, nil, :BIG_ENDIAN, :TRUNCATE)
    return s
  end

  def build_originating_transaction_id_message_contents(source_entity_id:, sequence_number:)
    id_length = read("ENTITY_ID_LENGTH")
    seq_num_length = read("SEQUENCE_NUMBER_LENGTH")
    s2 = define_originating_transaction_id_message_fixed_header()
    s2.write("RESERVED1", 0)
    s2.write("ENTITY_ID_LENGTH", id_length)
    s2.write("RESERVED2", 0)
    s2.write("SEQUENCE_NUMBER_LENGTH", seq_num_length)
    s3 = define_originating_transaction_id_message_variable_data(id_length: id_length + 1, seq_num_length: seq_num_length + 1)
    s3.write("SOURCE_ENTITY_ID", source_entity_id)
    s3.write("SEQUENCE_NUMBER", sequence_number)
    return s2.buffer(false) + s3.buffer(false)
  end

  def build_originating_transaction_id_message(source_entity_id:, sequence_number:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "ORIGINATING_TRANSACTION_ID")
    return s1.buffer(length) + build_originating_transaction_id_message_contents(source_entity_id: source_entity_id, sequence_number: sequence_number)
  end

  def decom_originating_transaction_id_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s = define_originating_transaction_id_message_fixed_header()
    s.buffer = message_to_user[0..(s.defined_length - 1)]
    id_length = s.read("ENTITY_ID_LENGTH")
    seq_num_length = s.read("SEQUENCE_NUMBER_LENGTH")
    message_to_user = message_to_user[s.defined_length..-1]
    s2 = define_originating_transaction_id_message_variable_data(id_length: id_length + 1, seq_num_length: seq_num_length + 1)
    s2.buffer = message_to_user[0..(s2.defined_length - 1)]
    result["SOURCE_ENTITY_ID"] = s2.read("SOURCE_ENTITY_ID")
    result["SEQUENCE_NUMBER"] = s2.read("SEQUENCE_NUMBER")
    message_to_user = message_to_user[s2.defined_length..-1]
    result["MSG_TYPE"] = "ORIGINATING_TRANSACTION_ID"
    return result, message_to_user
  end

  # Table 6-4
  def define_proxy_put_request_message
    id_length = read("ENTITY_ID_LENGTH") + 1
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s.append_item("LENGTH", 8, :UINT)
    s.append_item("DESTINATION_ENTITY_ID", id_length * 8, :UINT)
    return s, define_length_value(), define_length_value()
  end

  def build_proxy_put_request_message(destination_entity_id:, source_file_name: nil, destination_file_name: nil)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_PUT_REQUEST")
    dei, sfn, dfn = define_proxy_put_request_message()
    dei.write("LENGTH", read("ENTITY_ID_LENGTH") + 1)
    dei.write("DESTINATION_ENTITY_ID", destination_entity_id)
    sfn.write("LENGTH", source_file_name.to_s.length)
    sfn.write("VALUE", source_file_name.to_s)
    dfn.write("LENGTH", destination_file_name.to_s.length)
    dfn.write("VALUE", destination_file_name.to_s)
    return s1.buffer(false) + dei.buffer(false) + sfn.buffer(false) + dfn.buffer(false)
  end

  def decom_proxy_put_request_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    dei, sfn, dfn = define_proxy_put_request_message()
    dei.buffer = message_to_user[0..(dei.defined_length - 1)]
    result["DESTINATION_ENTITY_ID"] = dei.read("DESTINATION_ENTITY_ID")
    message_to_user = message_to_user[dei.defined_length..-1]
    sfn.buffer = message_to_user
    length = sfn.read("LENGTH")
    sfn.buffer = sfn.buffer(false)[0..length] # Includes length field
    source_file_name = sfn.read("VALUE")
    result["SOURCE_FILE_NAME"] = source_file_name if source_file_name.length > 0
    message_to_user = message_to_user[(length + 1)..-1]
    dfn.buffer = message_to_user
    length = dfn.read("LENGTH")
    dfn.buffer = dfn.buffer(false)[0..length] # Includes length field
    destination_file_name = dfn.read("VALUE")
    result["DESTINATION_FILE_NAME"] = destination_file_name if destination_file_name.length > 0
    result["MSG_TYPE"] = "PROXY_PUT_REQUEST"
    return result
  end

  # Table 6-5
  def define_proxy_message_to_user_message
    return define_length_value()
  end

  def build_proxy_message_to_user_message(message_to_user:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_MESSAGE_TO_USER")
    s2 = define_proxy_message_to_user_message()
    s2.write("LENGTH", message_to_user.length)
    s2.write("VALUE", message_to_user)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_message_to_user_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s = define_proxy_message_to_user_message()
    s.buffer = message_to_user
    length = s.read("LENGTH")
    if length > 0
      result["MESSAGE_TO_USER"] = s.read("VALUE")[0..(length - 1)]
    else
      result["MESSAGE_TO_USER"] = ""
    end
    result["MSG_TYPE"] = "PROXY_MESSAGE_TO_USER"
    return result
  end

  # Table 6-6
  def define_proxy_filestore_request_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s1.append_item("LENGTH", 8, :UINT)
    item = s1.append_item("ACTION_CODE", 4, :UINT)
    item.states = ACTION_CODES
    s1.append_item("SPARE", 4, :UINT)
    return s1, define_length_value(), define_length_value()
  end

  def build_proxy_filestore_request_message(action_code:, first_file_name:, second_file_name: nil)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_FILESTORE_REQUEST")
    s2, s3, s4 = define_proxy_filestore_request_message()
    s2.write("ACTION_CODE", action_code)
    s2.write("SPARE", 0)
    s3.write("LENGTH", first_file_name.to_s.length)
    s3.write("VALUE", first_file_name.to_s)
    s4.write("LENGTH", second_file_name.to_s.length)
    s4.write("VALUE", second_file_name.to_s)
    s2.write("LENGTH", s2.length + s3.length + s4.length)
    return s1.buffer(false) + s2.buffer(false) + s3.buffer(false) + s4.buffer(false)
  end

  def decom_proxy_filestore_request_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2, s3, s4 = define_proxy_filestore_request_message()
    s2.buffer = message_to_user[0..(s2.defined_length - 1)]
    message_to_user = message_to_user[s2.defined_length..-1]
    s3.buffer = message_to_user
    first_file_name_length = s3.read("LENGTH")
    s3.buffer = message_to_user[0..first_file_name_length]
    message_to_user = message_to_user[(first_file_name_length + 1)..-1]
    s4.buffer = message_to_user
    second_file_name_length = s4.read("LENGTH")
    s4.buffer = message_to_user[0..second_file_name_length]
    result["ACTION_CODE"] = s2.read("ACTION_CODE")
    result["FIRST_FILE_NAME"] = s3.read("VALUE") if first_file_name_length > 0
    result["SECOND_FILE_NAME"] = s4.read("VALUE") if second_file_name_length > 0
    result["MSG_TYPE"] = "PROXY_FILESTORE_REQUEST"
    return result
  end

  # Table 6-7
  def define_proxy_fault_handler_override_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    item = s1.append_item("HANDLER_CODE", 4, :UINT)
    item.states = HANDLER_CODES
    return s1
  end

  def build_proxy_fault_handler_override_message(condition_code:, handler_code:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_FAULT_HANDLER_OVERRIDE")
    s2 = define_proxy_fault_handler_override_message()
    s2.write("CONDITION_CODE", condition_code)
    s2.write("HANDLER_CODE", handler_code)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_fault_handler_override_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s1 = define_proxy_fault_handler_override_message()
    s1.buffer = message_to_user
    result["CONDITION_CODE"] = s1.read("CONDITION_CODE")
    result["HANDLER_CODE"] = s1.read("HANDLER_CODE")
    result["MSG_TYPE"] = "PROXY_FAULT_HANDLER_OVERRIDE"
    return result
  end

  # Table 6-8
  def define_proxy_transmission_mode_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("SPARE", 7, :UINT)
    item = s1.append_item("TRANSMISSION_MODE", 1, :UINT)
    item.states = TRANSMISSION_MODES
    return s1
  end

  def build_proxy_transmission_mode_message(transmission_mode:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_TRANSMISSION_MODE")
    s2 = define_proxy_transmission_mode_message()
    s2.write("SPARE", 0)
    s2.write("TRANSMISSION_MODE", transmission_mode)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_transmission_mode_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_proxy_transmission_mode_message()
    s2.buffer = message_to_user
    result["TRANSMISSION_MODE"] = s2.read("TRANSMISSION_MODE")
    result["MSG_TYPE"] = "PROXY_TRANSMISSION_MODE"
    return result
  end

  # Table 6-9
  def define_proxy_flow_label_message
    return define_length_value()
  end

  def build_proxy_flow_label_message(flow_label:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_FLOW_LABEL")
    s2 = define_proxy_flow_label_message()
    s2.write("LENGTH", flow_label.to_s.length)
    s2.write("VALUE", flow_label.to_s)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_flow_label_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_proxy_flow_label_message()
    s2.buffer = message_to_user
    length = s2.read("LENGTH")
    s2.buffer = message_to_user[0..length]
    result["FLOW_LABEL"] = s2.read("VALUE")
    result["MSG_TYPE"] = "PROXY_FLOW_LABEL"
    return result
  end

  # Table 6-10
  def define_proxy_segmentation_control_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("SPARE", 7, :UINT)
    item = s1.append_item("SEGMENTATION_CONTROL", 1, :UINT)
    item.states = SEGMENTATION_MODES
    return s1
  end

  def build_proxy_segmentation_control_message(segmentation_control:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_SEGMENTATION_CONTROL")
    s2 = define_proxy_segmentation_control_message()
    s2.write("SPARE", 0)
    s2.write("SEGMENTATION_CONTROL", segmentation_control)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_segmentation_control_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_proxy_segmentation_control_message()
    s2.buffer = message_to_user
    result["SEGMENTATION_CONTROL"] = s2.read("SEGMENTATION_CONTROL")
    result["MSG_TYPE"] = "PROXY_SEGMENTATION_CONTROL"
    return result
  end

  # Table 6-11
  def define_proxy_closure_request_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("SPARE", 7, :UINT)
    item = s1.append_item("CLOSURE_REQUESTED", 1, :UINT)
    item.states = CLOSURE_MODES
    return s1
  end

  def build_proxy_closure_request_message(closure_requested:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_CLOSURE_REQUEST")
    s2 = define_proxy_closure_request_message()
    s2.write("SPARE", 0)
    s2.write("CLOSURE_REQUESTED", closure_requested)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_closure_request_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_proxy_closure_request_message()
    s2.buffer = message_to_user
    result["CLOSURE_REQUESTED"] = s2.read("CLOSURE_REQUESTED")
    result["MSG_TYPE"] = "PROXY_CLOSURE_REQUEST"
    return result
  end

  # Table 6-12
  def define_proxy_put_response_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    s1.append_item("SPARE", 1, :UINT)
    item = s1.append_item("DELIVERY_CODE", 1, :UINT)
    item.states = DELIVERY_CODES
    item = s1.append_item("FILE_STATUS", 2, :UINT)
    item.states = FILE_STATUS_CODES
    return s1
  end

  def build_proxy_put_response_message(condition_code:, delivery_code:, file_status:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_PUT_RESPONSE")
    s2 = define_proxy_put_response_message()
    s2.write("CONDITION_CODE", condition_code)
    s2.write("DELIVERY_CODE", delivery_code)
    s2.write("FILE_STATUS", file_status)
    return s1.buffer(false) + s2.buffer(false)
  end

  def decom_proxy_put_response_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_proxy_put_response_message()
    result["CONDITION_CODE"] = s2.read("CONDITION_CODE")
    result["DELIVERY_CODE"] = s2.read("DELIVERY_CODE")
    result["FILE_STATUS"] = s2.read("FILE_STATUS")
    result["MSG_TYPE"] = "PROXY_PUT_RESPONSE"
    return result
  end

  # Table 6-13
  def define_proxy_filestore_response_message
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    s1.append_item("LENGTH", 8, :UINT)
    item = s1.append_item("ACTION_CODE", 4, :UINT)
    item.states = ACTION_CODES
    status_code_item = s1.append_item("STATUS_CODE", 4, :UINT)
    return s1, status_code_item, define_length_value(), define_length_value(), define_length_value()
  end

  def build_proxy_filestore_response_message(action_code:, status_code:, first_file_name:, second_file_name: nil, filestore_message:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_FILESTORE_RESPONSE")
    s2, status_code_item, s3, s4, s5 = define_proxy_filestore_response_message()
    s2.write("ACTION_CODE", action_code)
    CfdpPdu.add_status_code_states(action_code: action_code, status_code_item: status_code_item)
    s2.write("STATUS_CODE", status_code)
    s3.write("LENGTH", first_file_name.to_s.length)
    s3.write("VALUE", first_file_name.to_s)
    s4.write("LENGTH", second_file_name.to_s.length)
    s4.write("VALUE", second_file_name.to_s)
    s5.write("LENGTH", filestore_message.to_s.length)
    s5.write("VALUE", filestore_message.to_s)
    return s1.buffer(false) + s2.buffer(false) + s3.buffer(false) + s4.buffer(false) + s5.buffer(false)
  end

  def decom_proxy_filestore_response_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2, status_code_item, s3, s4, s5 = define_proxy_filestore_response_message()
    s2.buffer = message_to_user[0..(s2.defined_length - 1)]
    result["ACTION_CODE"] = s2.read("ACTION_CODE")
    result["STATUS_CODE"] = s2.read("STATUS_CODE")
    message_to_user = message_to_user[s2.defined_length..-1]
    s3.buffer = message_to_user
    length = s3.read("LENGTH")
    s3.buffer = s3.buffer(false)[0..length]
    first_file_name = s3.read("VALUE")
    result["FIRST_FILE_NAME"] = first_file_name if first_file_name.length > 0
    message_to_user = message_to_user[(length + 1)..-1]
    s4.buffer = message_to_user
    length = s4.read("LENGTH")
    s4.buffer = s4.buffer(false)[0..length]
    second_file_name = s4.read("VALUE")
    result["SECOND_FILE_NAME"] = second_file_name if second_file_name.length > 0
    message_to_user = message_to_user[(length + 1)..-1]
    s5.buffer = message_to_user
    length = s5.read("LENGTH")
    s5.buffer = s5.buffer(false)[0..length]
    filestore_message = s5.read("VALUE")
    result["FILESTORE_MESSAGE"] = filestore_message if filestore_message.length > 0
    result["MSG_TYPE"] = "PROXY_FILESTORE_RESPONSE"
    return result
  end

  # 6.2.6.2
  def build_proxy_put_cancel_message
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "PROXY_PUT_CANCEL")
    return s1.buffer(false)
  end

  def decom_proxy_put_cancel_message(message_to_user)
    result = {}
    result["MSG_TYPE"] = "PROXY_PUT_CANCEL"
    return result
  end

  # Table 6-15
  def define_directory_listing_request_message
    return define_length_value(), define_length_value()
  end

  def build_directory_listing_request_message(directory_name:, directory_file_name:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "DIRECTORY_LISTING_REQUEST")
    s2, s3 = define_directory_listing_request_message()
    s2.write("LENGTH", directory_name.to_s.length)
    s2.write("VALUE", directory_name.to_s)
    s3.write("LENGTH", directory_file_name.to_s.length)
    s3.write("VALUE", directory_file_name.to_s)
    return s1.buffer(false) + s2.buffer(false) + s3.buffer(false)
  end

  def decom_directory_listing_request_message(message_to_user)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    s2, s3 = define_directory_listing_request_message()
    s2.buffer = message_to_user
    length = s2.read("LENGTH")
    s2.buffer = s2.buffer(false)[0..length]
    directory_name = s2.read("VALUE")
    result["DIRECTORY_NAME"] = directory_name if directory_name.length > 0
    message_to_user = message_to_user[(length + 1)..-1]
    s3.buffer = message_to_user
    length = s3.read("LENGTH")
    s3.buffer = s3.buffer(false)[0..length]
    directory_file_name = s3.read("VALUE")
    result["DIRECTORY_FILE_NAME"] = directory_file_name if directory_file_name.length > 0
    result["MSG_TYPE"] = "DIRECTORY_LISTING_REQUEST"
    return result
  end

  # Table 6-16
  def define_directory_listing_response_message(version:)
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("RESPONSE_CODE", 1, :UINT)
    if version >= 1
      item.states = { "SUCCESSFUL" => 0, "UNSUCCESSFUL" => 1 }
    else
      item.states = { "SUCCESSFUL" => 0, "UNSUCCESSFUL" => 0xFF }
    end
    s1.append_item("SPARE", 7, :UINT)
    return s1, define_length_value(), define_length_value()
  end

  def build_directory_listing_response_message(response_code:, directory_name:, directory_file_name:, version:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "DIRECTORY_LISTING_RESPONSE")
    fixed, s2, s3 = define_directory_listing_response_message(version: version)
    fixed.write("RESPONSE_CODE", response_code)
    fixed.write("SPARE", 0)
    s2.write("LENGTH", directory_name.to_s.length)
    s2.write("VALUE", directory_name.to_s)
    s3.write("LENGTH", directory_file_name.to_s.length)
    s3.write("VALUE", directory_file_name.to_s)
    return s1.buffer(false) + fixed.buffer(false) + s2.buffer(false) + s3.buffer(false)
  end

  def decom_directory_listing_response_message(message_to_user, version:)
    result = {}
    message_to_user = message_to_user[5..-1] # Remove header
    fixed, s2, s3 = define_directory_listing_response_message(version: version)
    fixed.buffer = message_to_user[0..(fixed.defined_length - 1)]
    result["RESPONSE_CODE"] = fixed.read("RESPONSE_CODE")
    message_to_user = message_to_user[fixed.defined_length..-1]
    s2.buffer = message_to_user
    length = s2.read("LENGTH")
    s2.buffer = s2.buffer(false)[0..length]
    directory_name = s2.read("VALUE")
    result["DIRECTORY_NAME"] = directory_name if directory_name.length > 0
    message_to_user = message_to_user[(length + 1)..-1]
    s3.buffer = message_to_user
    length = s3.read("LENGTH")
    s3.buffer = s3.buffer(false)[0..length]
    directory_file_name = s3.read("VALUE")
    result["DIRECTORY_FILE_NAME"] = directory_file_name if directory_file_name.length > 0
    result["MSG_TYPE"] = "DIRECTORY_LISTING_RESPONSE"
    return result
  end

  # Table 6-18
  def define_remote_status_report_request_message_unique
    return define_length_value()
  end

  def build_remote_status_report_request_message(source_entity_id:, sequence_number:, report_file_name:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "REMOTE_STATUS_REPORT_REQUEST")
    data = build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
    s2 = define_remote_status_report_request_message_unique()
    s2.write("LENGTH", report_file_name.to_s.length)
    s2.write("VALUE", report_file_name.to_s)
    return s1.buffer(false) + data + s2.buffer(false)
  end

  def decom_remote_status_report_request_message(message_to_user)
    message_to_user = message_to_user[5..-1] # Remove header
    result, message_to_user = decom_originating_transaction_id_message(message_to_user)
    s2 = define_remote_status_report_request_message_unique()
    s2.buffer = message_to_user
    length = s2.read("LENGTH")
    s2.buffer = s2.buffer(false)[0..length]
    report_file_name = s2.read("VALUE")
    result["REPORT_FILE_NAME"] = report_file_name if report_file_name.length > 0
    result["MSG_TYPE"] = "REMOTE_STATUS_REPORT_REQUEST"
    return result
  end

  # Table 6-19
  def define_remote_status_report_response_message_unique
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("TRANSACTION_STATUS", 2, :UINT)
    item.states = TRANSACTION_STATUS_CODES
    s1.append_item("SPARE", 5, :UINT)
    item = s1.append_item("RESPONSE_CODE", 1, :UINT)
    item.states = { "SUCCESSFUL" => 0, "UNSUCCESSFUL" => 1 }
    return s1
  end

  def build_remote_status_report_response_message(source_entity_id:, sequence_number:, transaction_status:, response_code:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "REMOTE_STATUS_REPORT_RESPONSE")
    s2 = define_remote_status_report_response_message_unique()
    s2.write("TRANSACTION_STATUS", transaction_status)
    s2.write("SPARE", 0)
    s2.write("RESPONSE_CODE", response_code)
    data = build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
    return s1.buffer(false) + s2.buffer(false) + data
  end

  def decom_remote_status_report_response_message(message_to_user)
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_remote_status_report_response_message_unique()
    s2.buffer = message_to_user[0..(s2.defined_length - 1)]
    message_to_user = message_to_user[s2.defined_length..-1]
    result, message_to_user = decom_originating_transaction_id_message(message_to_user)
    result["TRANSACTION_STATUS"] = s2.read("TRANSACTION_STATUS")
    result["RESPONSE_CODE"] = s2.read("RESPONSE_CODE")
    result["MSG_TYPE"] = "REMOTE_STATUS_REPORT_RESPONSE"
    return result
  end

  # Table 6-21
  def build_remote_suspend_request_message(source_entity_id:, sequence_number:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "REMOTE_SUSPEND_REQUEST")
    data = build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
    return s1.buffer(false) + data
  end

  def decom_remote_suspend_request_message(message_to_user)
    message_to_user = message_to_user[5..-1] # Remove header
    result, message_to_user = decom_originating_transaction_id_message(message_to_user)
    result["MSG_TYPE"] = "REMOTE_SUSPEND_REQUEST"
    return result
  end

   # Table 6-22
  def define_remote_suspend_response_message_unique
    s1 = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s1.append_item("SUSPENSION_INDICATOR", 1, :UINT)
    item.states = { "NOT_SUSPENDED" => 0, "SUSPENDED" => 1 }
    item = s1.append_item("TRANSACTION_STATUS", 2, :UINT)
    item.states = TRANSACTION_STATUS_CODES
    s1.append_item("SPARE", 5, :UINT)
    return s1
  end

  def build_remote_suspend_response_message(source_entity_id:, sequence_number:, transaction_status:, suspension_indicator:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "REMOTE_SUSPEND_RESPONSE")
    s2 = define_remote_suspend_response_message_unique()
    s2.write("SUSPENSION_INDICATOR", suspension_indicator)
    s2.write("TRANSACTION_STATUS", transaction_status)
    s2.write("SPARE", 0)
    data = build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
    return s1.buffer(false) + s2.buffer(false) + data
  end

  def decom_remote_suspend_response_message(message_to_user)
    message_to_user = message_to_user[5..-1] # Remove header
    s2 = define_remote_suspend_response_message_unique()
    s2.buffer = message_to_user[0..(s2.defined_length - 1)]
    message_to_user = message_to_user[s2.defined_length..-1]
    result, message_to_user = decom_originating_transaction_id_message(message_to_user)
    result["SUSPENSION_INDICATOR"] = s2.read("SUSPENSION_INDICATOR")
    result["TRANSACTION_STATUS"] = s2.read("TRANSACTION_STATUS")
    result["MSG_TYPE"] = "REMOTE_SUSPEND_RESPONSE"
    return result
  end

  # Table 6-24
  def build_remote_resume_request_message(source_entity_id:, sequence_number:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "REMOTE_RESUME_REQUEST")
    data = build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
    return s1.buffer(false) + data
  end

  def decom_remote_resume_request_message(message_to_user)
    message_to_user = message_to_user[5..-1] # Remove header
    result, message_to_user = decom_originating_transaction_id_message(message_to_user)
    result["MSG_TYPE"] = "REMOTE_RESUME_REQUEST"
    return result
  end

  # Table 6-25
  def build_remote_resume_response_message(source_entity_id:, sequence_number:, transaction_status:, suspension_indicator:)
    s1 = define_reserved_cfdp_message_header()
    s1.write("MSG_ID", "cfdp")
    s1.write("MSG_TYPE", "REMOTE_RESUME_RESPONSE")
    s2 = define_remote_suspend_response_message_unique()
    s2.write("SUSPENSION_INDICATOR", suspension_indicator)
    s2.write("TRANSACTION_STATUS", transaction_status)
    s2.write("SPARE", 0)
    data = build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
    return s1.buffer(false) + s2.buffer(false) + data
  end

  def decom_remote_resume_response_message(message_to_user)
    result = decom_remote_suspend_response_message(message_to_user)
    result["MSG_TYPE"] = "REMOTE_RESUME_RESPONSE"
    return result
  end

  def decom_message_to_user(message_to_user, version:)
    s1 = define_reserved_cfdp_message_header()
    if message_to_user.length >= 5 # Minimum size
      s1.buffer = message_to_user[0..(s1.defined_length - 1)]
      if s1.read("MSG_ID") == 'cfdp'
        case s1.read("MSG_TYPE")
        when "PROXY_PUT_REQUEST"
          return decom_proxy_put_request_message(message_to_user)
        when "PROXY_MESSAGE_TO_USER"
          return decom_proxy_message_to_user_message(message_to_user)
        when "PROXY_FILESTORE_REQUEST"
          return decom_proxy_filestore_request_message(message_to_user)
        when "PROXY_FAULT_HANDLER_OVERRIDE"
          return decom_proxy_fault_handler_override_message(message_to_user)
        when "PROXY_TRANSMISSION_MODE"
          return decom_proxy_transmission_mode_message(message_to_user)
        when "PROXY_FLOW_LABEL"
          return decom_proxy_flow_label_message(message_to_user)
        when "PROXY_SEGMENTATION_CONTROL"
          return decom_proxy_segmentation_control_message(message_to_user)
        when "PROXY_PUT_RESPONSE"
          return decom_proxy_put_response_message(message_to_user)
        when "PROXY_FILESTORE_RESPONSE"
          return decom_proxy_filestore_response_message(message_to_user)
        when "PROXY_PUT_CANCEL"
          return decom_proxy_put_cancel_message(message_to_user)
        when "ORIGINATING_TRANSACTION_ID"
          result, message_to_user = decom_originating_transaction_id_message(message_to_user)
          return result
        when "PROXY_CLOSURE_REQUEST"
          return decom_proxy_closure_request_message(message_to_user)
        when "DIRECTORY_LISTING_REQUEST"
          return decom_directory_listing_request_message(message_to_user)
        when "DIRECTORY_LISTING_RESPONSE"
          return decom_directory_listing_response_message(message_to_user, version: version)
        when "REMOTE_STATUS_REPORT_REQUEST"
          return decom_remote_status_report_request_message(message_to_user)
        when "REMOTE_STATUS_REPORT_RESPONSE"
          return decom_remote_status_report_response_message(message_to_user)
        when "REMOTE_SUSPEND_REQUEST"
          return decom_remote_suspend_request_message(message_to_user)
        when "REMOTE_SUSPEND_RESPONSE"
          return decom_remote_suspend_response_message(message_to_user)
        when "REMOTE_RESUME_REQUEST"
          return decom_remote_resume_request_message(message_to_user)
        when "REMOTE_RESUME_RESPONSE"
          return decom_remote_resume_response_message(message_to_user)
        else
          return {"MSG_TYPE" => "UNKNOWN", "MSG_TYPE_VALUE" => s1.read("MSG_TYPE"), "DATA" => message_to_user}
        end
      else
        return {"MSG_TYPE" => "UNKNOWN", "DATA" => message_to_user}
      end
    else
      return {"MSG_TYPE" => "UNKNOWN", "DATA" => message_to_user}
    end
  end
end

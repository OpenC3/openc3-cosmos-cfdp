# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpPdu < OpenC3::Packet
  def self.decom_tlv(pdu, pdu_hash, variable_data)
    if variable_data.length >= 2 # Need at least 2 bytes
      s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
      item = s.append_item("TLV_TYPE", 8, :UINT)
      item.states = TLV_TYPES
      s.append_item("TLV_LENGTH", 8, :UINT)
      s.buffer = variable_data[0..(s.defined_length - 1)]
      type = s.read("TLV_TYPE")
      length = s.read("TLV_LENGTH")
      if length > 0
        tlv_data = variable_data[0..(length + 1)]
        variable_data = variable_data[(length + 2)..-1]
        tlv = {}
        tlv["TYPE"] = type
        pdu_hash["TLVS"] ||= []
        pdu_hash["TLVS"] << tlv

        case type
        when "FILESTORE_REQUEST"
          s, s2 = define_filestore_request_tlv()
          s.buffer = tlv_data
          first_file_name_length = s.read("FIRST_FILE_NAME_LENGTH")
          s.buffer = tlv_data[0..(4 + first_file_name_length - 1)]
          tlv_data = tlv_data[(4 + first_file_name_length)..-1]
          tlv["ACTION_CODE"] = s.read("ACTION_CODE")
          tlv["FIRST_FILE_NAME"] = s.read("FIRST_FILE_NAME")
          if tlv_data.length > 0
            s2.buffer = tlv_data
            second_file_name_length = s2.read("SECOND_FILE_NAME_LENGTH")
            s2.buffer = tlv_data[0..(1 + second_file_name_length - 1)]
            tlv["SECOND_FILE_NAME"] = s2.read("SECOND_FILE_NAME")
          end

        when "FILESTORE_RESPONSE"
          s, s2, s3, status_code_item = define_filestore_response_tlv()
          s.buffer = tlv_data
          first_file_name_length = s.read("FIRST_FILE_NAME_LENGTH")
          s.buffer = tlv_data[0..(4 + first_file_name_length - 1)]
          tlv_data = tlv_data[(4 + first_file_name_length)..-1]
          tlv["ACTION_CODE"] = s.read("ACTION_CODE")
          add_status_code_states(action_code: tlv["ACTION_CODE"], status_code_item: status_code_item)
          tlv["STATUS_CODE"] = s.read("STATUS_CODE")
          tlv["FIRST_FILE_NAME"] = s.read("FIRST_FILE_NAME")
          if tlv_data.length > 0
            s2.buffer = tlv_data
            second_file_name_length = s2.read("SECOND_FILE_NAME_LENGTH")
            s2.buffer = tlv_data[0..(1 + second_file_name_length - 1)]
            tlv_data = tlv_data[(1 + second_file_name_length)..-1]
            tlv["SECOND_FILE_NAME"] = s2.read("SECOND_FILE_NAME")
            if tlv_data.length > 0
              s3.buffer = tlv_data
              filestore_message_length = s3.read("FILESTORE_MESSAGE_LENGTH")
              s3.buffer = tlv_data[0..(1 + filestore_message_length - 1)]
              tlv_data = tlv_data[(1 + filestore_message_length)..-1]
              tlv["FILESTORE_MESSAGE"] = s3.read("FILESTORE_MESSAGE")
            end
          end

        when "MESSAGE_TO_USER"
          s = define_message_to_user_tlv()
          s.buffer = tlv_data
          tlv["MESSAGE_TO_USER"] = s.read("MESSAGE_TO_USER")

        when "FAULT_HANDLER_OVERRIDE"
          s = define_fault_handler_override_tlv()
          s.buffer = tlv_data
          tlv["CONDITION_CODE"] = s.read("CONDITION_CODE")
          tlv["HANDLER_CODE"] = s.read("HANDLER_CODE")

        when "FLOW_LABEL"
          s = define_flow_label_tlv()
          s.buffer = tlv_data
          tlv["FLOW_LABEL"] = s.read("FLOW_LABEL")

        when "ENTITY_ID"
          s = define_entity_id_tlv()
          s.buffer = tlv_data
          entity_id_string = s.read("ENTITY_ID")
          s2 = OpenC3::Structure.new(:BIG_ENDIAN)
          s2.append_item("ENTITY_ID", entity_id_string.length * 8, :UINT)
          s2.buffer = entity_id_string
          tlv["ENTITY_ID"] = s2.read("ENTITY_ID")

        end
      else
        variable_data = variable_data[2..-1]
      end
      return variable_data
    else
      return ""
    end
  end

  def self.build_tlv(tlv, entity_id_length: nil)
    type = tlv["TYPE"]
    case type
    when "FILESTORE_REQUEST"
      first_file_name = tlv["FIRST_FILE_NAME"].to_s
      second_file_name = nil
      length = 2 + first_file_name.length # type + length field + length
      if tlv.key?("SECOND_FILE_NAME")
        second_file_name = tlv["SECOND_FILE_NAME"].to_s
        length += 1 + second_file_name.length # length field + length
      end

      s, s2 = define_filestore_request_tlv()
      s.write("TLV_TYPE", "FILESTORE_REQUEST")
      s.write("TLV_LENGTH", length)
      s.write("ACTION_CODE", tlv['ACTION_CODE'])
      s.write("SPARE", 0)
      s.write("FIRST_FILE_NAME_LENGTH", first_file_name.length)
      s.write("FIRST_FILE_NAME", first_file_name)
      if second_file_name
        s2.write("SECOND_FILE_NAME_LENGTH", second_file_name.length)
        s2.write("SECOND_FILE_NAME", second_file_name)
        return s.buffer(false) + s2.buffer(false)
      else
        return s.buffer(false)
      end

    # TODO: Handled by cfdp_receive_transaction.rb, search 'Handle Filestore Requests'
    # when "FILESTORE_RESPONSE"
    #   first_file_name = tlv["FIRST_FILE_NAME"].to_s
    #   second_file_name = tlv["SECOND_FILE_NAME"].to_s
    #   filestore_message = tlv["FILESTORE_MESSAGE"].to_s

    #   s, s2, s3, status_code_item = define_filestore_response_tlv()
    #   add_status_code_states(action_code: tlv["ACTION_CODE"], status_code_item: status_code_item)
    #   s.write("TLV_TYPE", "FILESTORE_REQUEST")
    #   s.write("TLV_LENGTH", 4 + first_file_name.length + second_file_name.length + filestore_message.length)
    #   s.write("ACTION_CODE", tlv['ACTION_CODE'])
    #   s.write("STATUS_CODE", tlv['STATUS_CODE'])
    #   s.write("FIRST_FILE_NAME_LENGTH", first_file_name.length)
    #   s.write("FIRST_FILE_NAME", first_file_name)
    #   s2.write("SECOND_FILE_NAME_LENGTH", second_file_name.length)
    #   s2.write("SECOND_FILE_NAME", second_file_name)
    #   s3.write("FILESTORE_MESSAGE_LENGTH", filestore_message.length)
    #   s3.write("FILESTORE_MESSAGE", filestore_message)
    #   return s.buffer(false) + s2.buffer(false) + s3.buffer(false)

    when "MESSAGE_TO_USER"
      s = define_message_to_user_tlv()
      s.write("TLV_TYPE", "MESSAGE_TO_USER")
      s.write("TLV_LENGTH", tlv["MESSAGE_TO_USER"].length)
      s.write("MESSAGE_TO_USER", tlv["MESSAGE_TO_USER"])
      return s.buffer(false)

    when "FAULT_HANDLER_OVERRIDE"
      s = define_fault_handler_override_tlv()
      s.write("TLV_TYPE", "FAULT_HANDLER_OVERRIDE")
      s.write("TLV_LENGTH", 1)
      s.write("CONDITION_CODE", tlv["CONDITION_CODE"])
      s.write("HANDLER_CODE", tlv["HANDLER_CODE"])
      return s.buffer(false)

    when "FLOW_LABEL"
      s = define_flow_label_tlv()
      s.write("TLV_TYPE", "FLOW_LABEL")
      s.write("TLV_LENGTH", tlv["FLOW_LABEL"].length)
      s.write("FLOW_LABEL", tlv["FLOW_LABEL"])
      return s.buffer(false)

    when "ENTITY_ID"
      s = define_entity_id_tlv(entity_id_length: entity_id_length)
      s.write("TLV_TYPE", "ENTITY_ID")
      s.write("TLV_LENGTH", entity_id_length)
      s.write("ENTITY_ID", tlv["ENTITY_ID"])
      return s.buffer(false)

    end
  end

  # Table 5-15
  def self.define_filestore_request_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("TLV_TYPE", 8, :UINT) # 0x01
    item.states = TLV_TYPES
    s.append_item("TLV_LENGTH", 8, :UINT)
    item = s.append_item("ACTION_CODE", 4, :UINT)
    item.states = ACTION_CODES
    s.append_item("SPARE", 4, :UINT)
    s.append_item("FIRST_FILE_NAME_LENGTH", 8, :UINT)
    s.append_item("FIRST_FILE_NAME", 0, :BLOCK)

    s2 = OpenC3::Structure.new(:BIG_ENDIAN)
    s2.append_item("SECOND_FILE_NAME_LENGTH", 8, :UINT)
    s2.append_item("SECOND_FILE_NAME", 0, :BLOCK)

    return s, s2
  end

  # Table 5-17
  def self.define_filestore_response_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("TLV_TYPE", 8, :UINT) # 0x01
    item.states = TLV_TYPES
    s.append_item("TLV_LENGTH", 8, :UINT)
    item = s.append_item("ACTION_CODE", 4, :UINT)
    item.states = ACTION_CODES
    status_code_item = s.append_item("STATUS_CODE", 4, :UINT)
    s.append_item("FIRST_FILE_NAME_LENGTH", 8, :UINT)
    s.append_item("FIRST_FILE_NAME", 0, :BLOCK)

    s2 = OpenC3::Structure.new(:BIG_ENDIAN)
    s2.append_item("SECOND_FILE_NAME_LENGTH", 8, :UINT)
    s2.append_item("SECOND_FILE_NAME", 0, :BLOCK)

    s3 = OpenC3::Structure.new(:BIG_ENDIAN)
    s3.append_item("FILESTORE_MESSAGE_LENGTH", 8, :UINT)
    s3.append_item("FILESTORE_MESSAGE", 0, :BLOCK)

    return s, s2, s3, status_code_item
  end

  # Table 5-18
  def self.add_status_code_states(action_code:, status_code_item:)
    if String === action_code
      states = FILESTORE_RESPONSE_STATUS_CODES[action_code]
    else
      states = FILESTORE_RESPONSE_STATUS_CODES[ACTION_CODES.key(action_code)]
    end
    states = UNKNOWN_STATUS_CODES unless states
    status_code_item.states = states
  end

  # Section 5.4.3
  def self.define_message_to_user_tlv
    # Todo: Improve for Proxy support
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("TLV_TYPE", 8, :UINT) # 0x02
    item.states = TLV_TYPES
    s.append_item("TLV_LENGTH", 8, :UINT)
    s.append_item("MESSAGE_TO_USER", 0, :BLOCK)
    return s
  end

  # Table 5-19
  def self.define_fault_handler_override_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("TLV_TYPE", 8, :UINT) # 0x04
    item.states = TLV_TYPES
    s.append_item("TLV_LENGTH", 8, :UINT)
    item = s.append_item("CONDITION_CODE", 4, :UINT)
    item.states = CONDITION_CODES
    item = s.append_item("HANDLER_CODE", 4, :UINT)
    item.states = HANDLER_CODES
    return s
  end

  # Section 5.4.5
  def self.define_flow_label_tlv
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("TLV_TYPE", 8, :UINT) # 0x05
    item.states = TLV_TYPES
    s.append_item("TLV_LENGTH", 8, :UINT)
    s.append_item("FLOW_LABEL", 0, :BLOCK)
    return s
  end

  # See Section 5.4.6
  def self.define_entity_id_tlv(entity_id_length: nil)
    s = OpenC3::Packet.new(nil, nil, :BIG_ENDIAN)
    item = s.append_item("TLV_TYPE", 8, :UINT) # 0x06
    item.states = TLV_TYPES
    s.append_item("TLV_LENGTH", 8, :UINT)
    if entity_id_length
      s.append_item("ENTITY_ID", entity_id_length * 8, :UINT)
    else
      s.append_item("ENTITY_ID", 0, :BLOCK)
    end
    return s
  end
end

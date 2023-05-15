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

require 'thread'
require 'openc3/config/config_parser'
require 'openc3/packets/json_packet'
require 'openc3/utilities/logger'
require 'openc3/topics/topic'
require_relative 'cfdp_mib'
require_relative 'cfdp_receive_transaction'

class CfdpUser
  def initialize
    @thread = nil
    @cancel_thread = false
    @item_name_lookup = {}
    @source_transactions = []
    @source_threads = []

    at_exit do
      stop()
    end
  end

  def start
    @thread = Thread.new do
      begin
        source_entity = CfdpMib.source_entity
        topics = []

        tlm_packets = source_entity['tlm_info']
        tlm_packets.each do |target_name, packet_name, item_name|
          topic = "#{ENV['OPENC3_SCOPE']}__DECOM__{#{target_name.upcase}}__#{packet_name.upcase}"
          topics << topic
          @item_name_lookup[topic] = item_name.upcase
        end
        OpenC3::Topic.update_topic_offsets(topics)
        while !@cancel_thread
          # TODO: Handle freezing transactions if interface disconnects (or target goes unhealthy), and unfreezing if comes back to functional

          OpenC3::Topic.read_topics(topics) do |topic, msg_id, msg_hash, redis|
            break if @cancel_thread
            begin
              pdu_hash = receive_packet(topic, msg_id, msg_hash, redis)

              if pdu_hash['DIRECTION'] == "TOWARD_FILE_RECEIVER"
                if pdu_hash['DESTINATION_ENTITY_ID'] != CfdpMib.source_entity_id
                  OpenC3::Logger.error("Receiver PDU received for wrong entity: Mine: #{CfdpMib.source_entity_id}, Destination: #{pdu_hash['DESTINATION_ENTITY_ID']}", scope: ENV['OPENC3_SCOPE'])
                  next
                end
              else
                if pdu_hash['SOURCE_ENTITY_ID'] != CfdpMib.source_entity_id
                  OpenC3::Logger.error("Sender PDU received for wrong entity: Mine: #{CfdpMib.source_entity_id}, Source: #{pdu_hash['SOURCE_ENTITY_ID']}", scope: ENV['OPENC3_SCOPE'])
                  next
                end
              end

              transaction_id = CfdpTransaction.build_transaction_id(pdu_hash["SOURCE_ENTITY_ID"], pdu_hash["SEQUENCE_NUMBER"])
              transaction = CfdpMib.transactions[transaction_id]
              if transaction
                transaction.handle_pdu(pdu_hash)
              elsif pdu_hash["DIRECTIVE_CODE"] == "METADATA" or pdu_hash["DIRECTIVE_CODE"].nil?
                transaction = CfdpReceiveTransaction.new(pdu_hash) # Also calls handle_pdu inside
              else
                raise "Unknown transaction: #{transaction_id}, #{pdu_hash}"
              end
              if pdu_hash["DIRECTIVE_CODE"] == "METADATA" and not transaction.metadata_pdu_count > 1
                # Handle messages_to_user
                messages_to_user = []
                if pdu_hash["TLVS"]
                  pdu_hash["TLVS"].each do |tlv|
                    if tlv["TYPE"] == "MESSAGE_TO_USER"
                      messages_to_user << tlv
                    end
                  end
                end
                handle_messages_to_user(pdu_hash, messages_to_user) if messages_to_user.length > 0
              end
            rescue => err
              OpenC3::Logger.error(err.formatted, scope: ENV['OPENC3_SCOPE'])
            end
          end
          proxy_responses = []
          CfdpMib.transactions.dup.each do |transaction_id, transaction|
            transaction.update
            if transaction.proxy_response_needed
              # Send the proxy response
              params = {}
              params[:destination_entity_id] = transaction.proxy_response_info["SOURCE_ENTITY_ID"]
              params[:messages_to_user] = []
              destination_entity = CfdpMib.entity(Integer(params[:destination_entity_id]))
              pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
              params[:messages_to_user] << pdu.build_proxy_put_response_message(condition_code: transaction.condition_code, delivery_code: transaction.delivery_code, file_status: transaction.file_status)
              params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: transaction.proxy_response_info["SOURCE_ENTITY_ID"], sequence_number: transaction.proxy_response_info["SEQUENCE_NUMBER"])
              transaction.filestore_responses.each do |filestore_response|
                params[:messages_to_user] << pdu.build_proxy_filestore_response_message(action_code: filestore_response["ACTION_CODE"], status_code: filestore_response["STATUS_CODE"], first_file_name: filestore_response["FIRST_FILE_NAME"], second_file_name: filestore_response["SECOND_FILE_NAME"], filestore_message: filestore_response["FILESTORE_MESSAGE"])
              end
              proxy_responses << params
              transaction.proxy_response_needed = false
              transaction.proxy_response_info = nil
            end
          end
          proxy_responses.each do |params|
            start_source_transaction(params)
          end
        end
      rescue => err
        OpenC3::Logger.error(err.formatted, scope: ENV['OPENC3_SCOPE'])
      end
    end
  end

  def receive_packet(topic, msg_id, msg_hash, redis)
    topic_split = topic.gsub(/{|}/, '').split("__") # Remove the redis hashtag curly braces
    target_name = topic_split[2]
    packet_name = topic_split[3]
    stored = OpenC3::ConfigParser.handle_true_false(msg_hash["stored"])
    packet = OpenC3::JsonPacket.new(:TLM, target_name, packet_name, msg_hash["time"].to_i, stored, msg_hash['json_data'])
    pdu_data = packet.read(@item_name_lookup[topic])
    return CfdpPdu.decom(pdu_data)
  end

  def stop
    @cancel_thread = true
    @source_transactions.each do |t|
      t.abandon
    end
    @thread.join if @thread
    @thread = nil
    sleep(0.6) # Give threads time to die
    @source_threads.each do |st|
      st.kill if st.alive?
    end
  end

  def start_source_transaction(params, proxy_response_info: nil)
    transaction = CfdpSourceTransaction.new
    transaction.proxy_response_info = proxy_response_info
    @source_transactions << transaction
    @source_threads << Thread.new do
      begin
        if params[:remote_entity_id] and Integer(params[:remote_entity_id]) != CfdpMib.source_entity_id
          # Proxy Put
          destination_entity = CfdpMib.entity(Integer(params[:destination_entity_id]))
          pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
          messages_to_user = []
          # messages_to_user << pdu.build_originating_transaction_id_message(source_entity_id: CfdpMib.source_entity.id, sequence_number: transaction.transaction_seq_num)
          messages_to_user << pdu.build_proxy_put_request_message(destination_entity_id: Integer(params[:destination_entity_id]), source_file_name: params[:source_file_name], destination_file_name: params[:destination_file_name])
          if params[:messages_to_user]
            params[:messages_to_user].each do |message_to_user|
              messages_to_user << pdu.build_proxy_message_to_user_message(message_to_user: message_to_user)
            end
          end
          if params[:filestore_requests]
            params[:filestore_requests].each do |filestore_request|
              messages_to_user << pdu.build_proxy_filestore_request_message(action_code: filestore_request[0], first_file_name: filestore_request[1], second_file_name: filestore_request[2])
            end
          end
          if params[:fault_handler_overrides]
            params[:fault_handler_overrides].each do |fault_handler_override|
              messages_to_user << pdu.build_proxy_fault_handler_override_message(condition_code: fault_handler_override[0], handler_code: fault_handler_override[1])
            end
          end
          if params[:transmission_mode]
            messages_to_user << pdu.build_proxy_transmission_mode_message(transmission_mode: params[:transmission_mode])
          end
          if params[:flow_label]
            messages_to_user << pdu.build_proxy_flow_label_message(flow_label: params[:flow_label])
          end
          if params[:segmentation_control]
            messages_to_user << pdu.build_proxy_segmentation_control_message(segmentation_control: params[:segmentation_control])
          end
          if params[:closure_requested]
            messages_to_user << pdu.build_proxy_closure_request_message(closure_requested: params[:closure_requested])
          end
          OpenC3::Logger.info("CFDP Transaction #{transaction.id} Proxy Put to Remote Entity #{params[:remote_entity_id]}, Destination Entity #{params[:destination_entity_id]}\nSource File Name: #{params[:source_file_name]}\nDestination File Name: #{params[:destination_file_name]}", scope: ENV['OPENC3_SCOPE'])
          transaction.put(
            destination_entity_id: Integer(params[:remote_entity_id]),
            closure_requested: params[:closure_requested],
            transmission_mode: params[:transmission_mode],
            fault_handler_overrides: params[:fault_handler_overrides],
            messages_to_user: messages_to_user
          )
        else
          # Regular Put
          OpenC3::Logger.info("CFDP Transaction #{transaction.id} Put to Entity #{params[:destination_entity_id]}\nSource File Name: #{params[:source_file_name]}\nDestination File Name: #{params[:destination_file_name]}", scope: ENV['OPENC3_SCOPE'])
          transaction.put(
            destination_entity_id: Integer(params[:destination_entity_id]),
            source_file_name: params[:source_file_name],
            destination_file_name: params[:destination_file_name],
            transmission_mode: params[:transmission_mode],
            closure_requested: params[:closure_requested],
            filestore_requests: params[:filestore_requests],
            fault_handler_overrides: params[:fault_handler_overrides],
            messages_to_user: params[:messages_to_user],
            flow_label: params[:flow_label],
            segmentation_control: params[:segmentation_control]
          )
        end
      rescue => err
        OpenC3::Logger.error(err.formatted, scope: ENV['OPENC3_SCOPE'])
      end
    end
    return transaction
  end

  def proxy_request_setup(params)
    messages_to_user = []
    entity_id = Integer(params[:remote_entity_id])
    destination_entity = CfdpMib.entity(entity_id)
    pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
    return pdu, entity_id, messages_to_user
  end

  def proxy_request_start(entity_id:, messages_to_user:)
    params = {}
    params[:destination_entity_id] = entity_id
    params[:messages_to_user] = messages_to_user
    return start_source_transaction(params)
  end

  def start_directory_listing(params)
    raise "directory_name required" unless params[:directory_name] and params[:directory_name].length > 0
    raise "directory_file_name required" unless params[:directory_file_name] and params[:directory_file_name].length > 0
    pdu, entity_id, messages_to_user = proxy_request_setup(params)
    messages_to_user << pdu.build_directory_listing_request_message(directory_name: params[:directory_name], directory_file_name: params[:directory_file_name])
    return proxy_request_start(entity_id: entity_id, messages_to_user: messages_to_user)
  end

  def cancel(params)
    if params[:remote_entity_id] and Integer(params[:remote_entity_id]) != CfdpMib.source_entity_id
      # Proxy Cancel
      pdu, entity_id, messages_to_user = proxy_request_setup(params)
      source_entity_id, sequence_number = params[:transaction_id].split('__')
      messages_to_user << pdu.build_proxy_put_cancel_message()
      messages_to_user << pdu.build_originating_transaction_id_message(source_entity_id: Integer(source_entity_id), sequence_number: Integer(sequence_number))
      return proxy_request_start(entity_id: entity_id, messages_to_user: messages_to_user)
    else
      transaction = CfdpMib.transactions[params[:transaction_id]]
      if transaction
        transaction.cancel
        return transaction
      else
        return nil
      end
    end
  end

  def suspend(params)
    if params[:remote_entity_id] and Integer(params[:remote_entity_id]) != CfdpMib.source_entity_id
      # Proxy Suspend
      pdu, entity_id, messages_to_user = proxy_request_setup(params)
      source_entity_id, sequence_number = params[:transaction_id].split('__')
      messages_to_user << pdu.build_remote_suspend_request_message(source_entity_id: Integer(source_entity_id), sequence_number: Integer(sequence_number))
      return proxy_request_start(entity_id: entity_id, messages_to_user: messages_to_user)
    else
      transaction = CfdpMib.transactions[params[:transaction_id]]
      if transaction
        transaction.suspend
        return transaction
      else
        return nil
      end
    end
  end

  def resume(params)
    if params[:remote_entity_id] and Integer(params[:remote_entity_id]) != CfdpMib.source_entity_id
      # Proxy Resume
      pdu, entity_id, messages_to_user = proxy_request_setup(params)
      source_entity_id, sequence_number = params[:transaction_id].split('__')
      messages_to_user << pdu.build_remote_resume_request_message(source_entity_id: Integer(source_entity_id), sequence_number: Integer(sequence_number))
      return proxy_request_start(entity_id: entity_id, messages_to_user: messages_to_user)
    else
      transaction = CfdpMib.transactions[params[:transaction_id]]
      if transaction
        transaction.resume
        return transaction
      else
        return nil
      end
    end
  end

  def report(params)
    if params[:remote_entity_id] and Integer(params[:remote_entity_id]) != CfdpMib.source_entity_id
      raise "report_file_name required" unless params[:report_file_name] and params[:report_file_name].length > 0
      # Proxy Report
      pdu, entity_id, messages_to_user = proxy_request_setup(params)
      source_entity_id, sequence_number = params[:transaction_id].split('__')
      messages_to_user << pdu.build_remote_status_report_request_message(source_entity_id: Integer(source_entity_id), sequence_number: Integer(sequence_number), report_file_name: params[:report_file_name])
      return proxy_request_start(entity_id: entity_id, messages_to_user: messages_to_user)
    else
      transaction = CfdpMib.transactions[params[:transaction_id]]
      if transaction
        transaction.report
        return transaction
      else
        return nil
      end
    end
  end

  def handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    proxy_action = nil
    source_entity_id = nil
    sequence_number = nil
    request_source_entity_id = nil
    request_sequence_number = nil
    condition_code = nil
    delivery_code = nil
    file_status = nil
    filestore_responses = []
    directory_name = nil
    directory_file_name = nil
    response_code = nil
    transaction_status = nil
    suspension_indicator = nil
    report_file_name = nil

    params = {}
    params[:fault_handler_overrides] = []
    params[:messages_to_user] = []
    params[:filestore_requests] = []

    messages_to_user.each do |message_to_user|
      case message_to_user['MSG_TYPE']
      when "PROXY_PUT_REQUEST"
        params[:destination_entity_id] = message_to_user["DESTINATION_ENTITY_ID"]
        params[:source_file_name] = message_to_user["SOURCE_FILE_NAME"]
        params[:destination_file_name] = message_to_user["DESTINATION_FILE_NAME"]
        destination_entity = CfdpMib.entity(Integer(params[:destination_entity_id]))
        pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
        params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: metadata_pdu_hash["SEQUENCE_NUMBER"])
        proxy_action = :PUT

      when "PROXY_MESSAGE_TO_USER"
        params[:messages_to_user] << message_to_user["MESSAGE_TO_USER"]

      when "PROXY_FILESTORE_REQUEST"
        params[:filestore_requests] << message_to_user["ACTION_CODE", message_to_user["FIRST_FILE_NAME"]]
        params[:filestore_requests][-1] << message_to_user["SECOND_FILE_NAME"] if message_to_user["SECOND_FILE_NAME"]

      when "PROXY_FAULT_HANDLER_OVERRIDE"
        params[:fault_handler_overrides] << [message_to_user["CONDITION_CODE"], message_to_user["HANDLER_CODE"]]

      when "PROXY_TRANSMISSION_MODE"
        params[:transmission_mode] = message_to_user["TRANSMISSION_MODE"]

      when "PROXY_FLOW_LABEL"
        params[:flow_label] = message_to_user["FLOW_LABEL"]

      when "PROXY_SEGMENTATION_CONTROL"
        params[:segmentation_control] = message_to_user["SEGMENTATION_CONTROL"]

      when "PROXY_PUT_RESPONSE"
        # This is back at the originator
        condition_code = message_to_user["CONDITION_CODE"]
        delivery_code = message_to_user["DELIVERY_CODE"]
        file_status = message_to_user["FILE_STATUS"]
        proxy_action = :PUT_RESPONSE

      when "PROXY_FILESTORE_RESPONSE"
        # This is back at the originator
        filestore_responses << message_to_user.except("MSG_TYPE", "MSG_ID")

      when "PROXY_PUT_CANCEL"
        proxy_action = :CANCEL

      when "ORIGINATING_TRANSACTION_ID"
        source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        sequence_number = message_to_user["SEQUENCE_NUMBER"]

      when "PROXY_CLOSURE_REQUEST"
        params[:closure_requested] = message_to_user["CLOSURE_REQUESTED"]

      when "DIRECTORY_LISTING_REQUEST"
        proxy_action = :DIRECTORY_LISTING
        directory_name = message_to_user["DIRECTORY_NAME"]
        directory_file_name = message_to_user["DIRECTORY_FILE_NAME"]

      when "DIRECTORY_LISTING_RESPONSE"
        # This is back at the originator
        proxy_action = :DIRECTORY_LISTING_RESPONSE
        directory_name = message_to_user["DIRECTORY_NAME"]
        directory_file_name = message_to_user["DIRECTORY_FILE_NAME"]
        response_code = message_to_user["RESPONSE_CODE"]

      when "REMOTE_STATUS_REPORT_REQUEST"
        proxy_action = :REPORT
        request_source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        request_sequence_number = message_to_user["SEQUENCE_NUMBER"]
        report_file_name = message_to_user["REPORT_FILE_NAME"]

      when "REMOTE_STATUS_REPORT_RESPONSE"
        # This is back at the originator
        proxy_action = :REPORT_RESPONSE
        request_source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        request_sequence_number = message_to_user["SEQUENCE_NUMBER"]
        transaction_status = message_to_user["TRANSACTION_STATUS"]
        response_code = message_to_user["RESPONSE_CODE"]

      when "REMOTE_SUSPEND_REQUEST"
        proxy_action = :SUSPEND
        request_source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        request_sequence_number = message_to_user["SEQUENCE_NUMBER"]

      when "REMOTE_SUSPEND_RESPONSE"
        # This is back at the originator
        proxy_action = :SUSPEND_RESPONSE
        request_source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        request_sequence_number = message_to_user["SEQUENCE_NUMBER"]
        suspension_indicator = message_to_user["SUSPENSION_INDICATOR"]
        transaction_status = message_to_user["TRANSACTION_STATUS"]

      when "REMOTE_RESUME_REQUEST"
        proxy_action = :RESUME
        request_source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        request_sequence_number = message_to_user["SEQUENCE_NUMBER"]

      when "REMOTE_RESUME_RESPONSE"
        # This is back at the originator
        proxy_action = :RESUME_RESPONSE
        request_source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        request_sequence_number = message_to_user["SEQUENCE_NUMBER"]
        suspension_indicator = message_to_user["SUSPENSION_INDICATOR"]
        transaction_status = message_to_user["TRANSACTION_STATUS"]

      when "UNKNOWN"
        # Unknown Message - Ignore
        if message_to_user["DATA"].to_s.is_printable?
          OpenC3::Logger.warn("Received Unknown #{message_to_user["DATA"].length} Byte Message to User (#{message_to_user["MSG_TYPE_VALUE"]}): '#{message_to_user["DATA"].to_s}'", scope: ENV['OPENC3_SCOPE'])
        else
          OpenC3::Logger.warn("Received Unknown #{message_to_user["DATA"].length} Byte Message to User (#{message_to_user["MSG_TYPE_VALUE"]}): #{message_to_user["DATA"].to_s.simple_formatted}", scope: ENV['OPENC3_SCOPE'])
        end
      end
    end

    if proxy_action
      case proxy_action
      when :CANCEL
        CfdpMib.transactions.dup.each do |transaction_id, transaction|
          if transaction.proxy_response_info
            if transaction.proxy_response_info["SOURCE_ENTITY_ID"] == source_entity_id and transaction.proxy_response_info["SEQUENCE_NUMBER"] == sequence_number
              transaction.cancel(metadata_pdu_hash["SOURCE_ENTITY_ID"])
              break
            end
          end
        end

      when :PUT
        proxy_response_info = {
          "SOURCE_ENTITY_ID" => metadata_pdu_hash["SOURCE_ENTITY_ID"],
          "SEQUENCE_NUMBER" => metadata_pdu_hash["SEQUENCE_NUMBER"]
        }
        transaction = start_source_transaction(params, proxy_response_info: proxy_response_info)

      when :PUT_RESPONSE
        transaction_id = CfdpTransaction.build_transaction_id(source_entity_id, sequence_number)
        if filestore_responses.length > 0
          CfdpTopic.write_indication('Proxy-Put-Response',
            transaction_id: transaction_id, condition_code: condition_code,
            file_status: file_status, delivery_code: delivery_code,
            filestore_responses: filestore_responses)
        else
          CfdpTopic.write_indication('Proxy-Put-Response',
            transaction_id: transaction_id, condition_code: condition_code,
            file_status: file_status, delivery_code: delivery_code)
        end

      when :DIRECTORY_LISTING
        result = CfdpMib.directory_listing(directory_name, directory_file_name)
        if result
          params = {}
          params[:destination_entity_id] = metadata_pdu_hash["SOURCE_ENTITY_ID"]
          params[:source_file_name] = StringIO.new(result)
          if directory_file_name and directory_file_name.length > 0
            params[:destination_file_name] = directory_file_name
          else
            params[:destination_file_name] = "default_directory_file_name.txt"
          end
          params[:messages_to_user] = []
          destination_entity = CfdpMib.entity(metadata_pdu_hash["SOURCE_ENTITY_ID"])
          pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
          params[:messages_to_user] << pdu.build_directory_listing_response_message(response_code: "SUCCESSFUL", directory_name: directory_name, directory_file_name: directory_file_name)
          params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: metadata_pdu_hash["SEQUENCE_NUMBER"])
          start_source_transaction(params)
        else
          params = {}
          params[:destination_entity_id] = metadata_pdu_hash["SOURCE_ENTITY_ID"]
          params[:source_file_name] = nil
          params[:destination_file_name] = nil
          params[:messages_to_user] = []
          destination_entity = CfdpMib.entity(metadata_pdu_hash["SOURCE_ENTITY_ID"])
          pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
          params[:messages_to_user] << pdu.build_directory_listing_response_message(response_code: "UNSUCCESSFUL", directory_name: directory_name, directory_file_name: directory_file_name)
          params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: metadata_pdu_hash["SEQUENCE_NUMBER"])
          start_source_transaction(params)
        end

      when :DIRECTORY_LISTING_RESPONSE
        transaction_id = CfdpTransaction.build_transaction_id(source_entity_id, sequence_number)
        CfdpTopic.write_indication('Directory-Listing-Response',
          transaction_id: transaction_id, response_code: response_code,
          directory_name: directory_name, directory_file_name: directory_file_name)

      when :REPORT
        transaction_id = CfdpTransaction.build_transaction_id(request_source_entity_id, request_sequence_number)
        transaction = CfdpMib.transactions[transaction_id]
        if transaction
          params = {}
          params[:destination_entity_id] = metadata_pdu_hash["SOURCE_ENTITY_ID"]
          params[:source_file_name] = StringIO.new(transaction.build_report)
          if report_file_name and report_file_name.length > 0
            params[:destination_file_name] = report_file_name
          else
            params[:destination_file_name] = "default_report_file_name.txt"
          end
          params[:messages_to_user] = []
          destination_entity = CfdpMib.entity(metadata_pdu_hash["SOURCE_ENTITY_ID"])
          pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
          params[:messages_to_user] << pdu.build_remote_status_report_response_message(source_entity_id: request_source_entity_id, sequence_number: request_sequence_number, transaction_status: transaction.transaction_status, response_code: "SUCCESSFUL")
          params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: metadata_pdu_hash["SEQUENCE_NUMBER"])
          start_source_transaction(params)
        else
          params = {}
          params[:destination_entity_id] = metadata_pdu_hash["SOURCE_ENTITY_ID"]
          params[:source_file_name] = nil
          params[:destination_file_name] = nil
          params[:messages_to_user] = []
          destination_entity = CfdpMib.entity(metadata_pdu_hash["SOURCE_ENTITY_ID"])
          pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
          params[:messages_to_user] << pdu.build_remote_status_report_response_message(source_entity_id: request_source_entity_id, sequence_number: request_sequence_number, transaction_status: "UNDEFINED", response_code: "UNSUCCESSFUL")
          params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: metadata_pdu_hash["SEQUENCE_NUMBER"])
          start_source_transaction(params)
        end

      when :REPORT_RESPONSE
        transaction_id = CfdpTransaction.build_transaction_id(source_entity_id, sequence_number)
        CfdpTopic.write_indication('Remote-Report-Response',
          transaction_id: transaction_id, source_entity_id: request_source_entity_id, sequence_number: request_sequence_number,
          transaction_status: transaction_status, response_code: response_code)

      when :SUSPEND, :RESUME
        transaction_id = CfdpTransaction.build_transaction_id(request_source_entity_id, request_sequence_number)
        transaction = CfdpMib.transactions[transaction_id]
        suspension_indicator = "NOT_SUSPENDED"
        transaction_status = "UNDEFINED"
        if transaction
          if proxy_action == :SUSPEND
            transaction.suspend
          else
            transaction.resume
          end
          transaction_status = transaction.transaction_status
          suspension_indicator = "SUSPENDED" if transaction.state == "SUSPENDED"
        end
        params = {}
        params[:destination_entity_id] = metadata_pdu_hash["SOURCE_ENTITY_ID"]
        params[:source_file_name] = nil
        params[:destination_file_name] = nil
        params[:messages_to_user] = []
        destination_entity = CfdpMib.entity(metadata_pdu_hash["SOURCE_ENTITY_ID"])
        pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: destination_entity, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
        if proxy_action == :SUSPEND
          params[:messages_to_user] << pdu.build_remote_suspend_response_message(source_entity_id: request_source_entity_id, sequence_number: request_sequence_number, transaction_status: transaction_status, suspension_indicator: suspension_indicator)
        else
          params[:messages_to_user] << pdu.build_remote_resume_response_message(source_entity_id: request_source_entity_id, sequence_number: request_sequence_number, transaction_status: transaction_status, suspension_indicator: suspension_indicator)
        end
        params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: metadata_pdu_hash["SEQUENCE_NUMBER"])
        start_source_transaction(params)

      when :SUSPEND_RESPONSE
        transaction_id = CfdpTransaction.build_transaction_id(source_entity_id, sequence_number)
        CfdpTopic.write_indication('Remote-Suspend-Response',
          transaction_id: transaction_id, source_entity_id: request_source_entity_id, sequence_number: request_sequence_number,
          transaction_status: transaction_status, suspension_indicator: suspension_indicator)

      when :RESUME_RESPONSE
        transaction_id = CfdpTransaction.build_transaction_id(source_entity_id, sequence_number)
        CfdpTopic.write_indication('Remote-Resume-Response',
          transaction_id: transaction_id, source_entity_id: request_source_entity_id, sequence_number: request_sequence_number,
          transaction_status: transaction_status, suspension_indicator: suspension_indicator)

      end
    end
  end
end

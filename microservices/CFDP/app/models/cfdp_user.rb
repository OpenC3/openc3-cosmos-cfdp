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

              # TODO This only applies to certain directives
              #if pdu_hash['DESTINATION_ENTITY_ID'] != CfdpMib.source_entity_id
              #  raise "PDU received for different entity: Mine: #{CfdpMib.source_entity_id}, Destination: #{pdu_hash['DESTINATION_ENTITY_ID']}"
              #end

              transaction_id = CfdpTransaction.build_transaction_id(pdu_hash["SOURCE_ENTITY_ID"], pdu_hash["SEQUENCE_NUMBER"])
              transaction = CfdpMib.transactions[transaction_id]
              if transaction
                transaction.handle_pdu(pdu_hash)
              elsif pdu_hash["DIRECTIVE_CODE"] == "METADATA" or pdu_hash["DIRECTIVE_CODE"].nil?
                transaction = CfdpReceiveTransaction.new(pdu_hash) # Also calls handle_pdu inside
              else
                raise "Unknown transaction: #{transaction_id}, #{pdu_hash}"
              end
              if pdu_hash["DIRECTIVE_CODE"] == "METADATA" and not transaction.metadata_pdu_hash
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
          CfdpMib.transactions.each do |transaction_id, transaction|
            transaction.update
            # if transaction.proxy_response_needed
            #   # Send the proxy response
            #   params = {}
            #   params[:destination_entity_id] = transaction.proxy_response_info["SOURCE_ENTITY_ID"]
            #   params[:messages_to_user] = []
            #   pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: params[:destination_entity_id], file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
            #   params[:messages_to_user] << pdu.build_proxy_put_response_message(condition_code: transaction.condition_code, delivery_code: transaction.delivery_code, file_status: transaction.file_status)
            #   params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: transaction.proxy_response_info["SOURCE_ENTITY_ID"], sequence_number: transaction.proxy_response_info["SEQUENCE_NUMBER"])
            #   transaction.filestore_responses.each do |filestore_response|
            #     params[:messages_to_user] << pdu.build_proxy_filestore_response_message(action_code: filestore_response["ACTION_CODE"], status_code: filestore_response["STATUS_CODE"], first_file_name: filestore_response["FIRST_FILE_NAME"], second_file_name: filestore_response["SECOND_FILE_NAME"], filestore_message: filestore_response["FILESTORE_MESSAGE"])
            #   end
            #   start_source_transaction(params)
            #   transaction.proxy_response_needed = false
            #   transaction.proxy_response_info = nil
            # end
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

  def start_source_transaction(params)
    transaction = CfdpSourceTransaction.new
    @source_transactions << transaction
    @source_threads << Thread.new do
      begin
        if params[:source_entity_id] and params[:source_entity_id] != CfdpMib.source_entity_id
          # Proxy Put
          pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: params[:destination_entity_id], file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
          messages_to_user = []
          # messages_to_user << pdu.build_originating_transaction_id_message(source_entity_id: CfdpMib.source_entity.id, sequence_number: transaction.transaction_seq_num)
          messages_to_user << pdu.build_proxy_put_request_message(destination_entity_id: params[:destination_entity_id], source_file_name: params[:source_file_name], destination_file_name: params[:destination_file_name])
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
          transaction.put(
            destination_entity_id: params[:source_entity_id],
            closure_requested: params[:closure_requested],
            messages_to_user: messages_to_user,
          )
        else
          # Regular Put
          transaction.put(
            destination_entity_id: params[:destination_entity_id],
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

  def start_directory_listing(params)
    entity_id = params[:entity_id]
    pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: entity_id, file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
    messages_to_user = []
    messages_to_user << pdu.build_directory_listing_request_message(directory_name: params[:directory_name], directory_file_name: params[:directory_file_name])
    params = {}
    params[:destination_entity_id] = CfdpMib.source_entity_id
    params[:source_entity_id] = entity_id
    params[:messages_to_user] = messages_to_user
    return start_source_transaction(params)
  end

  def handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    messages_to_user.each do |message_to_user|
      proxy_action = nil
      source_entity_id = nil
      sequence_number = nil

      params = {}
      params[:fault_handler_overrides] = []
      params[:messages_to_user] = []
      params[:filestore_requests] = []

      case message_to_user['MSG_TYPE']
      when "PROXY_PUT_REQUEST"
        params[:destination_entity_id] = message_to_user["DESTINATION_ENTITY_ID"]
        params[:source_file_name] = message_to_user["SOURCE_FILE_NAME"]
        params[:destination_file_name] = message_to_user["DESTINATION_FILE_NAME"]
        pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: params[:destination_entity_id], file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
        params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: metadata_pdu_hash["SOURCE_ENTITY_ID"], sequence_number: meta_pdu_hash["SEQUENCE_NUMBER"])
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

      when "PROXY_FILESTORE_RESPONSE"
        # This is back at the originator

      when "PROXY_PUT_CANCEL"
        proxy_action = :CANCEL

      when "ORIGINATING_TRANSACTION_ID"
        source_entity_id = message_to_user["SOURCE_ENTITY_ID"]
        sequence_number = message_to_user["SEQUENCE_NUMBER"]

      when "PROXY_CLOSURE_REQUEST"
        params[:closure_requested] = message_to_user["CLOSURE_REQUESTED"]

      when "DIRECTORY_LISTING_REQUEST"

      when "DIRECTORY_LISTING_RESPONSE"
        # This is back at the originator

      when "REMOTE_STATUS_REPORT_REQUEST"

      when "REMOTE_STATUS_REPORT_RESPONSE"
        # This is back at the originator

      when "REMOTE_SUSPEND_REQUEST"

      when "REMOTE_SUSPEND_RESPONSE"
        # This is back at the originator

      when "REMOTE_RESUME_REQUEST"

      when "REMOTE_RESUME_RESPONSE"
        # This is back at the originator

      else

      end
    end

    if proxy_action
      case proxy_action
      when :CANCEL
        CfdpMib.transactions.each do |transaction_id, transaction|
          if transaction.proxy_response_info
            if transaction.proxy_response_info["SOURCE_ENTITY_ID"] == source_entity_id and transaction.proxy_response_info["SEQUENCE_NUMBER"] == sequence_number
              transaction.cancel
              break
            end
          end
        end
      when :PUT
        transaction = start_source_transaction(params)
        transaction.proxy_response_info = {
          "SOURCE_ENTITY_ID" => metadata_pdu_hash["SOURCE_ENTITY_ID"],
          "SEQUENCE_NUMBER" => metadata_pdu_hash["SEQUENCE_NUMBER"]
        }
      end
    end
  end
end

# params = {}
# params[:destination_entity_id] = source_entity_id
# params[:messages_to_user] = []
# pdu = CfdpPdu.build_initial_pdu(type: "FILE_DIRECTIVE", destination_entity: params[:destination_entity_id], file_size: 0, segmentation_control: "NOT_PRESERVED", transmission_mode: nil)
# params[:messages_to_user] << pdu.build_proxy_put_response_message(condition_code: transaction.condition_code, delivery_code: transaction.delivery_code, file_status: transaction.file_status)
# params[:messages_to_user] << pdu.build_originating_transaction_id_message(source_entity_id: source_entity_id, sequence_number: sequence_number)
# start_source_transaction(params)

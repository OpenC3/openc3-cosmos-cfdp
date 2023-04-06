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
          OpenC3::Topic.read_topics(topics) do |topic, msg_id, msg_hash, redis|
            break if @cancel_thread
            begin
              pdu_hash = receive_packet(topic, msg_id, msg_hash, redis)

              # TODO This only applies to certain directives
              #if pdu_hash['DESTINATION_ENTITY_ID'] != CfdpMib.source_entity_id
              #  raise "PDU received for different entity: Mine: #{CfdpMib.source_entity_id}, Destination: #{pdu_hash['DESTINATION_ENTITY_ID']}"
              #end

              transaction_id = CfdpReceiveTransaction.build_transaction_id(pdu_hash["SOURCE_ENTITY_ID"], pdu_hash["SEQUENCE_NUMBER"])
              transaction = CfdpMib.transactions[transaction_id]
              if transaction
                transaction.handle_pdu(pdu_hash)
              elsif pdu_hash["DIRECTIVE_CODE"] == "METADATA" or pdu_hash["DIRECTIVE_CODE"].nil?
                transaction = CfdpReceiveTransaction.new(pdu_hash)
              else
                raise "Unknown transaction: #{transaction_id}, #{pdu_hash}"
              end
            rescue => err
              OpenC3::Logger.error(err.formatted, scope: ENV['OPENC3_SCOPE'])
            end
          end
          CfdpMib.transactions.each do |transaction|
            transaction.update
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
    @thread.join if @thread
    @thread = nil
  end
end
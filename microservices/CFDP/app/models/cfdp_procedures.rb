# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

require 'openc3/packets/packet'
require 'openc3/utilities/store'
require 'openc3/topics/topic'
require 'openc3/api/api'

# Table 8-2: Remote Entity Configuration Information
# Remote entity ID
# Protocol version number
# UT address
# Positive ACK timer interval
# NAK timer interval
# Keep Alive interval
# Immediate NAK mode enabled
# Default transmission mode
# Transaction closure requested
# Check limit
# Default type of checksum to calculate for all file transmission to this remote entity
# Disposition of incomplete received file on transaction cancellation
# CRCs required on transmission
# Maximum file segment length
# Keep Alive discrepancy limit
# Positive ACK timer expiration limit
# NAK timer expiration limit
# Transaction inactivity limit
# Start of transmission opportunity
# End of transmission opportunity
# Start of reception opportunity
# End of reception opportunity

class CfdpMib
  KNOWN_FIELD_NAMES = [
    'protocol_version_number',
    'cmd_info',
    'tlm_info',
    'ack_timer_interval',
    'nak_timer_interval',
    'keep_alive_interval',
    'immediate_nak_mode',
    'default_transmission_mode',
    'transaction_closure_requested',
    'check_limit',
    'default_checksum_type',
    'incomplete_file_disposition',
    'crcs_required',
    'maximum_file_segment_length',
    'keep_alive_discrepancy_limit',
    'ack_timer_expiration_limit',
    'nak_timer_expiration_limit',
    'transaction_inactivity_limit',
    'entity_id_length',
    'sequence_number_length'
  ]

  @@source_entity_id = 0
  @@entities = {}
  @@bucket = nil
  @@root_path = "/"

  def self.entity(entity_id)
    return @@entities[entity_id]
  end

  def self.source_entity_id=(id)
    @@source_entity_id = id
  end

  def self.source_entity
    return @@entities[@@source_entity_id]
  end

  def self.bucket=(bucket)
    @@bucket = bucket
  end

  def self.bucket
    return @@bucket
  end

  def self.root_path=(root_path)
    @@root_path = root_path
  end

  def self.root_path
    @@root_path
  end

  def self.define_entity(entity_id)
    entity = {}
    entity['protocol_version_number'] = 0
    # These two settings map to UT address in COSMOS
    entity['cmd_info'] = nil
    entity['tlm_info'] = []
    entity['ack_timer_interval'] = 600
    entity['nak_timer_interval'] = 600
    entity['keep_alive_interval'] = 600
    entity['immediate_nak_mode'] = false
    entity['default_transmission_mode'] = 'UNACKNOWLEDGED'
    entity['transaction_closure_requested'] = false
    entity['check_limit'] = 0
    entity['default_checksum_type'] = 0
    entity['incomplete_file_disposition'] = "DISCARD"
    entity['crcs_required'] = true
    entity['maximum_file_segment_length'] = 1024
    entity['keep_alive_discrepancy_limit'] = 16
    entity['ack_timer_expiration_limit'] = 1
    entity['nak_timer_expiration_limit'] = 1
    entity['transaction_inactivity_limit'] = 1
    entity['entity_id_length'] = 0 # 0 = 1 byte
    entity['sequence_number_length'] = 0 # 0 = 1 byte
    # TODO: Use interface connected? to limit opportunities?
    @@entities[entity_id.to_i] = entity
    return entity
  end

  def self.set_entity_value(entity_id, field_name, value)
    field_name = field_name.downcase
    raise "Unknown OPTION #{field_name}" unless KNOWN_FIELD_NAMES.include?(field_name)
    case field_name
    when 'tlm_info'
      @@entities[entity_id.to_i][field_name] << value
    else
      @@entities[entity_id.to_i][field_name] = value
    end
  end

  def self.get_source_file(source_file_name)

  end

  def self.setup
    # Get options for our microservice
    model = OpenC3::MicroserviceModel.get_model(ENV['OPENC3_MICROSERVICE_NAME'], scope: ENV['OPENC3_SCOPE'])

    # Initialize MIB from OPTIONS
    current_entity_id = nil
    source_entity_defined = false
    destination_entity_defined = false
    root_path_defined = false
    models.options.each do |option|
      field_name = option[0].to_s.downcase
      value = option[1..-1]
      value = value[0] if value.length == 1
      case field_name
      when 'source_entity'
        source_entity_defined = true
        current_entity_id = value
        CfdpMib.define_entity(current_entity_id)
        CfdpMib.source_entity_id = current_entity_id
      when 'destination_entity'
        destination_entity_defined = true
        current_entity_id = value
        CfdpMib.define_entity(current_entity_id)
      when 'bucket'
        CfdpMib.bucket = value
      when 'root_path'
        root_path_defined = true
        CfdpMib.root_path = value
      else
        if current_entity_id
          CfdpMib.set_entity_value(current_entity_id, field_name, value)
        else
          raise "Must declare 'source_entity entity_id or destination_entity entity_id' before other options"
        end
      when
    end

    raise "OPTION source_entity is required" unless source_entity_defined
    raise "OPTION destination_entity is required" unless destination_entity_defined
    raise "OPTION root_path is required" unless root_path_defined
  end
end

class CfdpUser
  def initialize
    @thread = nil
    @cancel_thread = false
    @item_name_lookup = {}
    @transactions = {}
  end

  def start
    @thread = Thread.new do
      source_entity = CfdpMib.source_entity
      topics = []

      tlm_packets = source_entity['tlm_info']
      tlm_packets.each do |target_name, packet_name, item_name|
        topic = "#{ENV['OPENC3_SCOPE']}__DECOM__{#{target_name.upcase}}__#{packet_name.upcase}"
        topics << topic
        @item_name_lookup[topic] = item_name.upcase
      end
      Topic.update_topic_offsets(topics)
      while !@cancel_thread
        Topic.read_topics(topics) do |topic, msg_id, msg_hash, redis|
          break if @cancel_thread
          receive_packet(topic, msg_id, msg_hash, redis)
        end
      end
    end
  end

  def receive_packet(topic, msg_id, msg_hash, redis)
    topic_split = topic.gsub(/{|}/, '').split("__") # Remove the redis hashtag curly braces
    target_name = topic_split[2]
    packet_name = topic_split[3]
    stored = ConfigParser.handle_true_false(msg_hash["stored"])
    packet = JsonPacket.new(:TLM, target_name, packet_name, msg_hash["time"].to_i, stored, msg_hash['json_data'])
    pdu_data = packet.read(@item_name_lookup[topic])
    return CfdpPdu.decom(pdu_data)
  end

  def stop
    @cancel_thread = true
    @thread.join if @thread
    @thread = nil
  end
end

class CfdpModel
  def self.get_next_transaction_seq_num
    key = "cfdp/#{ENV['OPENC3_MICROSERVICE_NAME']}/transaction_seq_num"
    transaction_seq_num = OpenC3::Store.incr(key)
    return transaction_seq_num
  end
end

class CfdpTopic < Topic
  def self.write_indication(indication_type, **kw_args)
    msg_hash = {
      :time => Time.now.to_nsec_from_epoch,
      :indication_type => indication_type,
    }
    kw_args.each do |key, value|
      msg_hash[key] = value
    end
    Topic.write_topic("#{ENV['OPENC3_MICROSERVICE_NAME']}__CFDP", msg_hash)
  end
end

class CfdpChecksum
  def initialize
    @checksum = 0
  end

  def add(offset, data)
    front_pad_bytes = offset % 4
    if front_pad_bytes != 0
      data = ("\x00" * front_pad_bytes) << data
    end
    end_pad_bytes = 4 - data.length % 4
    if end_pad_bytes != 4
      data = data + ("\x00" * end_pad_bytes)
    end
    values = data.unpack('N*')
    values.each do |value|
      @checksum += value
    end
    return @checksum
  end

  def checksum
    return @checksum & 0xFFFFFFFF
  end
end

def NullChecksum
  def add(offset, data)
    return 0
  end

  def checksum
    return 0
  end
end

class CfdpProcedures
  include OpenC3::Api

  def put(
    destination_entity_id:,
    source_file_name: nil,
    destination_file_name: nil,
    segmentation_control: nil, # Not supported
    fault_handler_overrides: [],
    flow_label: nil, # Not supported
    transmission_mode: nil,
    closure_requested: nil,
    messages_to_user: [],
    filestore_requests: [])

    transaction_seq_num, transaction_id = transaction_start_notification()
    copy_file(
      transaction_seq_num: transaction_seq_num,
      transaction_id: transaction_id,
      destination_entity_id: destination_entity_id,
      source_file_name: source_file_name,
      destination_file_name: destination_file_name,
      segmentation_control: segmentation_control, # Not supported
      fault_handler_overrides: fault_handler_overrides,
      flow_label: flow_label, # Not supported
      transmission_mode: transmission_mode,
      closure_requested: closure_requested,
      messages_to_user: messages_to_user,
      filestore_requests: filestore_requests
    )
  end

  def transaction_start_notification(source_entity)
    # Issue Next Transaction Id
    transaction_seq_num = CfdpModel.get_next_transaction_seq_num
    transaction_id = "#{source_entity['id']}__#{transaction_seq_num}"

    # Issue Transaction.indication
    CfdpTopic.write_indication("Transaction", transaction_id: transaction_id)

    return transaction_seq_num, transaction_id
  end

  def copy_file(
    transaction_seq_num:,
    transaction_id:,
    destination_entity_id:,
    source_file_name:,
    destination_file_name:,
    segmentation_control: "NOT_PRESERVED", # Not supported
    fault_handler_overrides:,
    flow_label:, # Not supported
    transmission_mode:,
    closure_requested:,
    messages_to_user:,
    filestore_requests:)

    # Lookup outgoing PDU command
    source_entity = CfdpMib.source_entity
    destination_entity = CfdpMib.entity(destination_entity_id)
    target_name, packet_name, item_name = destination_entity["cmd_info"]

    # Prepare file
    source_file = CfdpMib.get_source_file(source_file_name)
    file_size = source_file.size
    read_size = destination_entity['maximum_file_segment_length']

    # Send Metadata PDU
    metadata_pdu = CfdpPdu.build_metadata_pdu(
      source_entity: source_entity,
      transaction_seq_num: transaction_seq_num,
      destination_entity: destination_entity,
      closure_requested: closure_requested,
      file_size: file_size,
      source_file_name: source_file_name,
      destination_file_name: destination_file_name,
      options: [],
      segmentation_control: segmentation_control,
      transmission_mode: transmission_mode)
    cmd_params = {}
    cmd_params[item_name] = metadata_pdu.buffer(false)
    cmd(target_name, packet_name, cmd_params)

    checksum = get_checksum(destination_entity)

    # Send File Data PDUs
    offset = 0
    while true
      file_data = source_file.read(read_size)
      break if file_data.nil? or file_data.length <= 0
      file_data_pdu = CfdpPdu.build_file_data_pdu(
        offset: offset,
        file_data: file_data,
        file_size: file_size,
        source_entity: source_entity,
        transaction_seq_num: transaction_seq_num,
        destination_entity: destination_entity,
        segmentation_control: segmentation_control,
        transmission_mode: transmission_mode)
      cmd_params = {}
      cmd_params[item_name] = file_data_pdu.buffer(false)
      cmd(target_name, packet_name, cmd_params)
      checksum.add(offset, file_data)
      offset += file_data.length
    end

    # Send EOF PDU
    eof_pdu = CfdpPdu.build_eof_pdu(
      source_entity: source_entity,
      transaction_seq_num: transaction_seq_num,
      destination_entity: destination_entity,
      file_size: file_size,
      file_checksum: checksum.checksum,
      condition_code: "NO_ERROR",
      segmentation_control: segmentation_control,
      transmission_mode: transmission_mode,
      canceling_entity_id: nil)
    cmd_params = {}
    cmd_params[item_name] = eof_pdu.buffer(false)
    cmd(target_name, packet_name, cmd_params)

    # Issue EOF-Sent.indication
    CfdpTopic.write_indication("EOF-Sent", transaction_id: transaction_id)
  end

  # private

  def get_checksum(entity)
    checksum_type = entity['default_checksum_type']
    if checksum_type == 15
      return NullChecksum.new
    else
      return CfdpChecksum.new
    end
  end
end

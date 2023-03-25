require_relative 'cfdp_model'
require_relative 'cfdp_receive_transaction'
require_relative 'cfdp_mib'
require_relative 'cfdp_topic'
require_relative 'cfdp_pdu'
require_relative 'cfdp_checksum'
require_relative 'cfdp_null_checksum'

class CfdpSourceTransaction
  include OpenC3::Api

  attr_reader :id

  def initialize(source_entity: nil)
    @source_entity = CfdpMib.source_entity unless source_entity
    raise "No source entity defined" unless @source_entity
    @transaction_seq_num = CfdpModel.get_next_transaction_seq_num
    @id = CfdpReceiveTransaction.build_transaction_id(@source_entity['id'], @transaction_seq_num)
    CfdpMib.transactions[@id] = self
    @finished_pdu_hash = nil
  end

  def put(
    destination_entity_id:,
    source_file_name: nil,
    destination_file_name: nil,
    segmentation_control: "NOT_PRESERVED", # Not supported
    fault_handler_overrides: [],
    flow_label: nil, # Not supported
    transmission_mode: nil,
    closure_requested: nil,
    messages_to_user: [],
    filestore_requests: [])

    raise "destination_entity_id is required" if destination_entity_id.nil?
    destination_entity_id = Integer(destination_entity_id)

    transaction_start_notification()
    copy_file(
      transaction_seq_num: @transaction_seq_num,
      transaction_id: @id,
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

  def transaction_start_notification
    # Issue Transaction.indication
    CfdpTopic.write_indication("Transaction", transaction_id: @id)
  end

  def copy_file(
    transaction_seq_num:,
    transaction_id:,
    destination_entity_id:,
    source_file_name:,
    destination_file_name:,
    segmentation_control: "NOT_PRESERVED", # Not supported
    fault_handler_overrides:,
    flow_label: nil, # Not supported
    transmission_mode:,
    closure_requested:,
    messages_to_user:,
    filestore_requests:)

    # Lookup outgoing PDU command
    source_entity = CfdpMib.source_entity
    destination_entity = CfdpMib.entity(destination_entity_id)
    raise "Unknown destination entity: #{destination_entity_id}" unless destination_entity
    target_name, packet_name, item_name = destination_entity["cmd_info"]
    raise "cmd_info not configured for destination_entity: #{destination_entity_id}" unless target_name and packet_name and item_name

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
    cmd_params[item_name] = metadata_pdu
    cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])

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
      cmd_params[item_name] = file_data_pdu
      cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      checksum.add(offset, file_data)
      offset += file_data.length
    end

    # Send EOF PDU
    @condition_code = "NO_ERROR"
    eof_pdu = CfdpPdu.build_eof_pdu(
      source_entity: source_entity,
      transaction_seq_num: transaction_seq_num,
      destination_entity: destination_entity,
      file_size: file_size,
      file_checksum: checksum.checksum,
      condition_code: @condition_code,
      segmentation_control: segmentation_control,
      transmission_mode: transmission_mode,
      canceling_entity_id: nil)
    cmd_params = {}
    cmd_params[item_name] = eof_pdu
    cmd(target_name, packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])

    # Issue EOF-Sent.indication
    CfdpTopic.write_indication("EOF-Sent", transaction_id: transaction_id)

    @file_status = "UNREPORTED"
    @delivery_code = "DATA_COMPLETE"

    # Wait for Finished if Closure Requested
    if closure_requested == "CLOSURE_REQUESTED"
      start_time = Time.now
      while (Time.now - start_time) < source_entity['check_limit']
        sleep(1)
        break if @finished_pdu_hash
      end
      if @finished_pdu_hash
        @file_status = @finished_pdu_hash['FILE_STATUS']
        @delivery_code = @finished_pdu_hash['DELIVERY_CODE']
        @condition_code = @finished_pdu_hash['CONDITION_CODE']
      else
        @condition_code = "CHECK_LIMIT_REACHED"
      end
    end

    CfdpTopic.write_indication("Transaction-Finished", transaction_id: transaction_id, condition_code: @condition_code, file_status: @file_status, delivery_code: @delivery_code)
  end

  def handle_pdu(pdu_hash)
    case pdu_hash["DIRECTIVE_CODE"]
    when "EOF"

    when "FINISHED"
      @finished_pdu_hash = pdu_hash

    when "ACK"

    when "METADATA"

    when "NAK"

    when "PROMPT"

    when "KEEP_ALIVE"

    else # File Data

    end
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

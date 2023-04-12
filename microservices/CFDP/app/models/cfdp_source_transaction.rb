require_relative 'cfdp_transaction'

class CfdpSourceTransaction < CfdpTransaction

  def initialize(source_entity: nil)
    super()
    @source_entity = source_entity
    @source_entity = CfdpMib.source_entity unless source_entity
    raise "No source entity defined" unless @source_entity
    @transaction_seq_num = CfdpModel.get_next_transaction_seq_num
    @id = CfdpReceiveTransaction.build_transaction_id(@source_entity['id'], @transaction_seq_num)
    CfdpMib.transactions[@id] = self
    @finished_pdu_hash = nil
    @source_file_name = nil
    @destination_file_name = nil
    @destination_entity = nil
    @eof_count = 0
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

    @source_file_name = source_file_name
    @destination_file_name = destination_file_name
    @segmentation_control = segmentation_control
    @segmentation_control = "NOT_PRESERVED" unless @segmentation_control
    fault_handler_overrides = [] unless fault_handler_overrides
    messages_to_user = [] unless messages_to_user
    filestore_requests = [] unless filestore_requests

    begin
      transaction_start_notification()
      copy_file(
        transaction_seq_num: @transaction_seq_num,
        transaction_id: @id,
        destination_entity_id: destination_entity_id,
        source_file_name: @source_file_name,
        destination_file_name: @destination_file_name,
        fault_handler_overrides: fault_handler_overrides,
        flow_label: flow_label, # Not supported
        transmission_mode: transmission_mode,
        closure_requested: closure_requested,
        messages_to_user: messages_to_user,
        filestore_requests: filestore_requests
      )
    rescue => err
      # TODO: Gracefully cancel on fatal exceptions
      raise err
    end
  end

  def transaction_start_notification
    # Issue Transaction.indication
    CfdpTopic.write_indication("Transaction", transaction_id: @id)
  end

  def handle_suspend
    while @status == "SUSPENDED" or @freeze
      sleep(1)
    end
  end

  def update
    if @status != "SUSPENDED"
      if @eof_ack_timeout and Time.now > @eof_ack_timeout
        # Resend eof pdu
        cmd_params = {}
        cmd_params[@item_name] = @eof_pdu
        cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
        @eof_count += 1
        if @eof_count > CfdpMib.source_entity['ack_timer_expiration_limit']
          # Positive ACK Limit Reached Fault
          @condition_code = "ACK_LIMIT_REACHED"
          handle_fault()
          @eof_ack_timeout = nil
        else
          @eof_ack_timeout = Time.now + CfdpMib.source_entity['ack_timer_interval']
        end
      end
    end
  end

  def copy_file(
    transaction_seq_num:,
    transaction_id:,
    destination_entity_id:,
    source_file_name:,
    destination_file_name:,
    fault_handler_overrides:,
    flow_label: nil, # Not supported
    transmission_mode:,
    closure_requested:,
    messages_to_user:,
    filestore_requests:)

    # Lookup outgoing PDU command
    @source_entity = CfdpMib.source_entity
    @destination_entity = CfdpMib.entity(destination_entity_id)
    raise "Unknown destination entity: #{destination_entity_id}" unless @destination_entity
    @transmission_mode = transmission_mode
    @transmission_mode = @destination_entity['default_transmission_mode'].upcase unless @transmission_mode
    @target_name, @packet_name, @item_name = @destination_entity["cmd_info"]
    raise "cmd_info not configured for destination_entity: #{destination_entity_id}" unless @target_name and @packet_name and @item_name

    if source_file_name and destination_file_name
      # Prepare file
      source_file = CfdpMib.get_source_file(source_file_name)
      file_size = source_file.size
      read_size = @destination_entity['maximum_file_segment_length']
    else
      source_file = nil
      file_size = 0
    end

    # Prepare options
    options = []
    filestore_requests = [] unless filestore_requests
    filestore_requests.each do |fsr|
      tlv = {}
      tlv["TYPE"] = "FILESTORE_REQUEST"
      tlv["ACTION_CODE"] = fsr[0].to_s.upcase
      tlv["FIRST_FILE_NAME"] = fsr[1]
      tlv["SECOND_FILE_NAME"] = fsr[2] if fsr[2]
      options << tlv
    end

    fault_handler_overrides = [] unless fault_handler_overrides
    fault_handler_overrides.each do |fho|
      tlv = {}
      tlv["TYPE"] = "FAULT_HANDLER_OVERRIDE"
      tlv["CONDITION_CODE"] = fho[0].to_s.upcase
      tlv["HANDLER_CODE"] = fho[1].to_s.upcase
      options << tlv
      @fault_handler_overrides[tlv["CONDITION_CODE"]] = tlv["HANDLER_CODE"]
    end

    handle_suspend()
    return if @status == "ABANDONED"

    # Send Metadata PDU
    @metadata_pdu = CfdpPdu.build_metadata_pdu(
      source_entity: @source_entity,
      transaction_seq_num: @transaction_seq_num,
      destination_entity: @destination_entity,
      closure_requested: closure_requested,
      file_size: file_size,
      source_file_name: source_file_name,
      destination_file_name: destination_file_name,
      options: options,
      segmentation_control: @segmentation_control,
      transmission_mode: @transmission_mode)
    cmd_params = {}
    cmd_params[@item_name] = @metadata_pdu
    cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])

    if source_file
      checksum = get_checksum(@destination_entity['default_checksum_type'])
      unless checksum
        # Unsupported algorithm - Use modular instead
        @condition_code = "UNSUPPORTED_CHECKSUM_TYPE"
        handle_fault()
        checksum = CfdpChecksum.new
      end

      # Send File Data PDUs
      offset = 0
      while true
        break if @status == "CANCELED"
        handle_suspend()
        return if @status == "ABANDONED"
        file_data = source_file.read(read_size)
        break if file_data.nil? or file_data.length <= 0
        file_data_pdu = CfdpPdu.build_file_data_pdu(
          offset: offset,
          file_data: file_data,
          file_size: file_size,
          source_entity: @source_entity,
          transaction_seq_num: @transaction_seq_num,
          destination_entity: @destination_entity,
          segmentation_control: @segmentation_control,
          transmission_mode: @transmission_mode)
        cmd_params = {}
        cmd_params[@item_name] = file_data_pdu
        cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
        checksum.add(offset, file_data)
        offset += file_data.length
        @progress = offset
      end
    end

    handle_suspend()
    return if @status == "ABANDONED"

    # Send EOF PDU
    if source_file
      file_checksum = checksum.checksum(source_file, false)
    else
      file_checksum = 0
    end
    if @canceling_entity_id
      @condition_code = "CANCEL_REQUEST_RECEIVED"
      eof_file_size = @progress
    else
      eof_file_size = file_size
    end
    begin
      @eof_pdu = CfdpPdu.build_eof_pdu(
        source_entity: @source_entity,
        transaction_seq_num: @transaction_seq_num,
        destination_entity: @destination_entity,
        file_size: eof_file_size,
        file_checksum: file_checksum,
        condition_code: @condition_code,
        segmentation_control: @segmentation_control,
        transmission_mode: @transmission_mode,
        canceling_entity_id: @canceling_entity_id)
      cmd_params = {}
      cmd_params[@item_name] = @eof_pdu
      cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
    rescue => err
      abandon() if @canceling_entity_id
      raise err
    end

    # Issue EOF-Sent.indication
    CfdpTopic.write_indication("EOF-Sent", transaction_id: transaction_id)

    @eof_ack_timeout = Time.now + CfdpMib.source_entity['ack_timer_interval'] if @transmission_mode == "ACKNOWLEDGED"

    @file_status = "UNREPORTED"
    @delivery_code = "DATA_COMPLETE"

    # Wait for Finished if Closure Requested or Acknowledged Mode
    if closure_requested == "CLOSURE_REQUESTED" or @transmission_mode == "ACKNOWLEDGED"
      start_time = Time.now
      while (Time.now - start_time) < @source_entity['check_limit']
        sleep(1)
        break if @finished_pdu_hash
      end
      if @finished_pdu_hash
        @file_status = @finished_pdu_hash['FILE_STATUS']
        @delivery_code = @finished_pdu_hash['DELIVERY_CODE']
        @condition_code = @finished_pdu_hash['CONDITION_CODE'] unless @canceling_entity_id
      else
        unless @canceling_entity_id
          @condition_code = "CHECK_LIMIT_REACHED"
          handle_fault()
        end
      end
    end

    # Complete use of source file
    CfdpMib.complete_source_file(source_file) if source_file

    notice_of_completion()
  end

  def notice_of_completion
    # Cancel all timeouts
    @eof_ack_timeout = nil

    filestore_responses = []
    if @finished_pdu_hash
      tlvs = @finished_pdu_hash["TLVS"]
      if tlvs
        tlvs.each do |tlv|
          case tlv['TYPE']
          when 'FILESTORE_RESPONSE'
            filestore_responses << tlv.except('TYPE')
          end
        end
      end
    end
    @status = "FINISHED" unless @status == "CANCELED" or @status == "ABANDONED"

    if filestore_responses.length > 0
      CfdpTopic.write_indication("Transaction-Finished",
        transaction_id: @id, condition_code: @condition_code,
        file_status: @file_status, delivery_code: @delivery_code, status_report: @status,
        filestore_responses: filestore_responses)
    else
      CfdpTopic.write_indication("Transaction-Finished",
        transaction_id: @id, condition_code: @condition_code,
        file_status: @file_status, status_report: @status, delivery_code: @delivery_code)
    end
  end

  def handle_pdu(pdu_hash)
    case pdu_hash["DIRECTIVE_CODE"]
    when "EOF", "METADATA", "PROMPT"
      # Unexpected - Ignore

    when "FINISHED"
      @finished_pdu_hash = pdu_hash

      if @finished_pdu_hash["CONDITION_CODE"] == "CANCEL_REQUEST_RECEIVED" and @status != "CANCELED"
        cancel(@destination_entity.id)
      end

      if @transmission_mode == "ACKNOWLEDGED"
        # Ack Finished PDU
        ack_pdu = CfdpPdu.build_ack_pdu(
          source_entity: @source_entity,
          transaction_seq_num: @transaction_seq_num,
          destination_entity: @destination_entity,
          segmentation_control: @segmentation_control,
          transmission_mode: @transmission_mode,
          condition_code: @finished_pdu_hash["CONDITION_CODE"],
          ack_directive_code: "FINISHED",
          transaction_status: "ACTIVE")
        cmd_params = {}
        cmd_params[@item_name] = ack_pdu
        cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      end

    when "ACK"
      # EOF Ack
      @eof_ack_pdu_hash = pdu_hash
      @eof_ack_timeout = nil

    when "NAK"
      handle_nak(pdu_hash)

    when "KEEP_ALIVE"
      @keep_alive_pdu_hash = pdu_hash
      if (@progress - @keep_alive_pdu_hash['PROGRESS']) > @destination_entity['keep_alive_discrepancy_limit']
        @condition_code = "KEEP_ALIVE_LIMIT_REACHED"
        handle_fault()
      end
    else # File Data
      # Unexpected - Ignore
    end
  end

  def handle_nak(pdu_hash)
    source_file = CfdpMib.get_source_file(source_file_name)
    file_size = source_file.size
    max_read_size = @destination_entity['maximum_file_segment_length']

    pdu_hash["SEGMENT_REQUESTS"].each do |request|
      start_offset = request["START_OFFSET"]
      end_offset = request["END_OFFSET"]

      if start_offset == 0 and end_offset == 0
        # Send Metadata PDU
        cmd_params = {}
        cmd_params[@item_name] = @metadata_pdu
        cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
      else
        # Send File Data PDU(s)
        offset = start_offset
        source_file.seek(offset, IO::SEEK_SET)
        while true
          bytes_remaining = end_offset - offset
          break if bytes_remaining <= 0
          if bytes_remaining >= max_read_size
            read_size = max_read_size
          else
            read_size = bytes_remaining
          end
          file_data = source_file.read(read_size)
          break if file_data.nil? or file_data.length <= 0
          file_data_pdu = CfdpPdu.build_file_data_pdu(
            offset: offset,
            file_data: file_data,
            file_size: file_size,
            source_entity: @source_entity,
            transaction_seq_num: @transaction_seq_num,
            destination_entity: @destination_entity,
            segmentation_control: @segmentation_control,
            transmission_mode: @transmission_mode)
          cmd_params = {}
          cmd_params[@item_name] = file_data_pdu
          cmd(@target_name, @packet_name, cmd_params, scope: ENV['OPENC3_SCOPE'])
          offset += file_data.length
        end
      end
    end

    CfdpMib.complete_source_file(source_file) if source_file
  end
end

require 'openc3/api/api'
require_relative 'cfdp_model'
require_relative 'cfdp_mib'
require_relative 'cfdp_topic'
require_relative 'cfdp_pdu'
require_relative 'cfdp_checksum'
require_relative 'cfdp_null_checksum'
require_relative 'cfdp_crc_checksum'
require 'tempfile'

class CfdpTransaction
  include OpenC3::Api
  attr_reader :id
  attr_reader :frozen
  attr_reader :status
  attr_reader :progress

  def initialize
    @frozen = false
    @status = "ACTIVE" # ACTIVE, FINISHED, CANCELED, SUSPENDED
    @progress = 0
    @condition_code = "NO_ERROR"
    @canceling_entity_id = nil
    @fault_handler_overrides = {}
  end

  def suspend
    if @status == "ACTIVE"
      @condition_code = "SUSPEND_REQUEST_RECEIVED"
      @status = "SUSPENDED"
      CfdpTopic.write_indication("Suspended", transaction_id: transaction_id, condition_code: @condition_code)
    end
  end

  def resume
    if @status == "SUSPENDED"
      @status = "ACTIVE"
      @condition_code = "NO_ERROR"
      @inactivity_timeout = Time.now + CfdpMib.source_entity['keep_alive_interval']
      CfdpTopic.write_indication("Resumed", transaction_id: transaction_id, progress: @progress)
    end
  end

  def cancel(entity_id = nil)
    if @status != "FINISHED"
      @condition_code = "CANCEL_REQUEST_RECEIVED" if @condition_code == "NO_ERROR"
      if entity_id
        @canceling_entity_id = entity_id
      else
        @canceling_entity_id = CfdpMib.source_entity.id
      end
      @status = "CANCELED"
    end
  end

  def abandon
    if @status != "FINISHED"
      @status = "ABANDONED"
      CfdpTopic.write_indication("Abandoned", transaction_id: @id, condition_code: @condition_code, progress: @progress)
    end
  end

  def report
    CfdpTopic.write_indication("Report", transaction_id: @id, status_report: @status)
  end

  def freeze
    @freeze = true
  end

  def unfreeze
    @freeze = false
  end

  def handle_fault
    if @fault_handler_overrides[@condition_code]
      case @fault_handler_overrides[@condition_code]
      when "ISSUE_NOTICE_OF_CANCELATION"
        cancel()
      when "ISSUE_NOTICE_OF_SUSPENSION"
        suspend()
      when "IGNORE_ERROR"
        ignore_fault()
      when "ABONDON_TRANSACTION"
        abandon()
      end
    else
      case @condition_code
      when "ACK_LIMIT_REACHED"
        ignore_fault()
      when "KEEP_ALIVE_LIMIT_REACHED"
        ignore_fault()
      when "INVALID_TRANSMISSION_MODE"
        ignore_fault()
      when "FILESTORE_REJECTION"
        ignore_fault()
      when "FILE_CHECKSUM_FAILURE"
        ignore_fault()
      when "FILE_SIZE_ERROR"
        ignore_fault()
      when "NAK_LIMIT_REACHED"
        ignore_fault()
      when "INACTIVITY_DETECTED"
        cancel()
      when "INVALID_FILE_STRUCTURE"
        ignore_fault()
      when "CHECK_LIMIT_REACHED"
        ignore_fault()
      when "UNSUPPORTED_CHECKSUM_TYPE"
        ignore_fault()
      end
    end
  end

  def ignore_fault
    CfdpTopic.write_indication("Fault", transaction_id: @id, condition_code: @condition_code, progress: @progress)
  end

  def update
    # Default do nothing
  end

  def get_checksum(checksum_type)
    case checksum_type
    when 0 # Modular Checksum
      return CfdpChecksum.new
    when 1 # Proximity-1 CRC-32 - Poly: 0x00A00805 - Reference CCSDS-211.2-B-3 - Unsure of correct xor/reflect
      return CfdpCrcChecksum.new(0x00A00805, 0x00000000, false, false)
    when 2 # CRC-32C - Poly: 0x1EDC6F41 - Reference RFC4960
      return CfdpCrcChecksum.new(0x1EDC6F41, 0xFFFFFFFF, true, true)
    when 3 # CRC-32 - Poly: 0x04C11DB7 - Reference Ethernet Frame Check Sequence
      return CfdpCrcChecksum.new(0x04C11DB7, 0xFFFFFFFF, true, true)
    when 15
      return CfdpNullChecksum.new
    else # Unsupported
      return nil
    end
  end
end

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
  end

  def suspend
    if @status == "ACTIVE"
      @status = "SUSPENDED"
      CfdpTopic.write_indication("Suspended", transaction_id: transaction_id, condition_code: @condition_code)
    end
  end

  def resume
    if @status == "SUSPENDED"
      @status = "ACTIVE"
      CfdpTopic.write_indication("Resumed", transaction_id: transaction_id, progress: @progress)
    end
  end

  def cancel
    if @status != "FINISHED"
      @status = "CANCELED"
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

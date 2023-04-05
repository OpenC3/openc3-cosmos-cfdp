require 'openc3/api/api'
require_relative 'cfdp_model'
require_relative 'cfdp_receive_transaction'
require_relative 'cfdp_mib'
require_relative 'cfdp_topic'
require_relative 'cfdp_pdu'
require_relative 'cfdp_checksum'
require_relative 'cfdp_null_checksum'
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

  def get_checksum(entity)
    checksum_type = entity['default_checksum_type']
    if checksum_type == 15
      return NullChecksum.new
    else
      return CfdpChecksum.new
    end
  end
end

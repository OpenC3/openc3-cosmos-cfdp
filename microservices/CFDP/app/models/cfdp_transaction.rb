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

  def initialize
    @frozen = false
    @status = "ACTIVE" # ACTIVE, FINISHED, CANCELED, SUSPENDED
  end

  def suspend
    if @status == "ACTIVE"
      @status = "SUSPENDED"
    end
  end

  def resume
    if @status = "SUSPENDED"
      @status = "ACTIVE"
    end
  end

  def cancel
    if @status != "FINISHED"
      @status = "CANCELED"
    end
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

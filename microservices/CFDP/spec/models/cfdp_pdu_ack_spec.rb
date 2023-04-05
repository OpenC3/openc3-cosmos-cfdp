# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

require 'rails_helper'
require 'cfdp_pdu'
require 'openc3/models/microservice_model'
require 'openc3/utilities/store_autoload'

RSpec.describe CfdpPdu, type: :model do
  before(:each) do
    mock_redis()
    @source_entity_id = 1
    @destination_entity_id = 2
    ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
    # Create the model that is consumed by CfdpMib.setup
    model = OpenC3::MicroserviceModel.new(name: ENV['OPENC3_MICROSERVICE_NAME'], scope: "DEFAULT",
      options: [
        ["source_entity_id", @source_entity_id],
        ["destination_entity_id", @destination_entity_id],
        ["root_path", SPEC_DIR],
      ],
    )
    model.create
    CfdpMib.setup
  end

  # Validate Table 5-8: ACK PDU Contents
  describe "build_ack_pdu" do
    it "builds a ACK PDU with Finished" do
      buffer = CfdpPdu.build_ack_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        condition_code: "NO_ERROR", # 0
        segmentation_control: "NOT_PRESERVED",
        ack_directive_code: "FINISHED", # 5
        transaction_status: "TERMINATED") # 2
      # puts buffer.formatted
      expect(buffer.length).to eql 12

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 6 # ACK per Table 5-4
      # Directive Code of ACK PDU
      expect(buffer[8].unpack('C')[0] >> 4).to eql 5 # Finished
      # Directive Subtype Code
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 1
      # Condition Code
      expect(buffer[9].unpack('C')[0] >> 4).to eql 0
      # Transaction Status
      expect(buffer[9].unpack('C')[0] & 0x3).to eql 2

      hash = {}
      # decom takes just the ACK specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_ack_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['ACK_DIRECTIVE_CODE']).to eql 'FINISHED'
      expect(hash['ACK_DIRECTIVE_SUBTYPE']).to eql 1
      expect(hash['CONDITION_CODE']).to eql 'NO_ERROR'
      expect(hash['TRANSACTION_STATUS']).to eql 'TERMINATED'
    end

    it "builds a ACK PDU with EOF" do
      buffer = CfdpPdu.build_ack_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        condition_code: "ACK_LIMIT_REACHED", # 1
        segmentation_control: "NOT_PRESERVED",
        ack_directive_code: "EOF", # 4
        transaction_status: "UNRECOGNIZED") # 3
      # puts buffer.formatted
      expect(buffer.length).to eql 12

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 6 # ACK per Table 5-4
      # Directive Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 4
      # Directive Subtype Code
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0
      # Condition Code
      expect(buffer[9].unpack('C')[0] >> 4).to eql 1
      # Transaction Status
      expect(buffer[9].unpack('C')[0] & 0x3).to eql 3

      hash = {}
      # decom takes just the ACK specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_ack_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['ACK_DIRECTIVE_CODE']).to eql 'EOF'
      expect(hash['ACK_DIRECTIVE_SUBTYPE']).to eql 0
      expect(hash['CONDITION_CODE']).to eql 'ACK_LIMIT_REACHED'
      expect(hash['TRANSACTION_STATUS']).to eql 'UNRECOGNIZED'
    end
  end
end

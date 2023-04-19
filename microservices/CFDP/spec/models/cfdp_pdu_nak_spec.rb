# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# Licensed for Evaluation and Educational Use
#
# This file may only be used commercially under the terms of a commercial license
# purchased from OpenC3, Inc.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# The development of this software was funded in-whole or in-part by MethaneSAT LLC.

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

  # Validate Table 5-10: NAK PDU Contents
  describe "build_nak_pdu" do
    it "builds a NAK PDU with no segement requests" do
      buffer = CfdpPdu.build_nak_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        start_of_scope: 0x12345678,
        end_of_scope: 0xABCDEFFF,
        segment_requests: [])
      # puts buffer.formatted
      expect(buffer.length).to eql 18

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 8 # NAK per Table 5-4
      # Start of scope
      expect(buffer[8..11].unpack('N')[0]).to eql 0x12345678
      # End of scope
      expect(buffer[12..15].unpack('N')[0]).to eql 0xABCDEFFF

      hash = {}
      # decom takes just the NAK specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_nak_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['START_OF_SCOPE']).to eql 0x12345678
      expect(hash['END_OF_SCOPE']).to eql 0xABCDEFFF
      expect(hash['SEGMENT_REQUESTS']).to eql []
    end

    # Validate Table 5-11: Segment Request Form
    it "builds a NAK PDU with segement requests" do
      buffer = CfdpPdu.build_nak_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        start_of_scope: 0,
        end_of_scope: 0x1F,
        segment_requests: [
          [0, 0xF],
          [0x10, 0x1F],
        ])
      # puts buffer.formatted
      expect(buffer.length).to eql 34

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 8 # NAK per Table 5-4
      # Start of scope
      expect(buffer[8..11].unpack('N')[0]).to eql 0
      # End of scope
      expect(buffer[12..15].unpack('N')[0]).to eql 0x1F
      # Segment requests
      expect(buffer[16..19].unpack('N')[0]).to eql 0
      expect(buffer[20..23].unpack('N')[0]).to eql 0xF
      expect(buffer[24..27].unpack('N')[0]).to eql 0x10
      expect(buffer[28..31].unpack('N')[0]).to eql 0x1F

      hash = {}
      # decom takes just the NAK specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_nak_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['START_OF_SCOPE']).to eql 0
      expect(hash['END_OF_SCOPE']).to eql 0x1F
      expect(hash['SEGMENT_REQUESTS']).to eql [
        {
          "START_OFFSET" => 0,
          "END_OFFSET" => 0xF,
        },
        {
          "START_OFFSET" => 0x10,
          "END_OFFSET" => 0x1F,
        }
      ]
    end
  end
end

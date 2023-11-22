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

  # Validate Table 5-6: End-of-File PDU Contents
  describe "build_eof_pdu" do
    it "builds a EOF PDU with no error" do
      buffer = CfdpPdu.build_eof_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xBA5EBA11,
        file_checksum: 0xDEADBEEF,
        condition_code: "NO_ERROR", # 0
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        canceling_entity_id: nil)
      # puts buffer.formatted
      expect(buffer.length).to eql 19

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number
      expect(buffer[1..2].unpack('n')[0]).to eql 12 # PDU_DATA_LENGTH - Directive Code plus Data plus CRC

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 4 # EOF per Table 5-4
      # Condition Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 0
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # Spare
      # File Checksum
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # File Size
      expect(buffer[13..16].unpack('N')[0]).to eql 0xBA5EBA11

      hash = {}
      # decom takes just the EOF specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_eof_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CONDITION_CODE']).to eql "NO_ERROR"
      expect(hash['FILE_CHECKSUM']).to eql 0xDEADBEEF
      expect(hash['FILE_SIZE']).to eql 0xBA5EBA11
    end

    it "builds a EOF PDU with large file size" do
      buffer = CfdpPdu.build_eof_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0x100000000,
        file_checksum: 0xDEADBEEF,
        condition_code: "NO_ERROR", # 0
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        canceling_entity_id: nil)
      # puts buffer.formatted
      expect(buffer.length).to eql 23

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 4 # EOF per Table 5-4
      # Condition Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 0
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # Spare
      # File Checksum
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # File Size
      expect(buffer[13..16].unpack('N')[0]).to eql 1

      hash = {}
      # decom takes just the EOF specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      pdu = CfdpPdu.new(crcs_required: false)
      # Set the LARGE_FILE flag so we properly decom
      pdu.write("LARGE_FILE_FLAG", "LARGE_FILE")
      CfdpPdu.decom_eof_pdu_contents(pdu, hash, buffer[8..-3])
      expect(hash['CONDITION_CODE']).to eql "NO_ERROR"
      expect(hash['FILE_CHECKSUM']).to eql 0xDEADBEEF
      expect(hash['FILE_SIZE']).to eql 0x100000000
    end

    it "builds a EOF PDU with cancellation status" do
      buffer = CfdpPdu.build_eof_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xBA5EBA11,
        file_checksum: 0xDEADBEEF,
        condition_code: "CANCEL_REQUEST_RECEIVED", # 15
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        canceling_entity_id: 0x5)
      # puts buffer.formatted
      expect(buffer.length).to eql 22

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 4 # EOF per Table 5-4
      # Condition Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 15
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # Spare
      # File Checksum
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # File Size
      expect(buffer[13..16].unpack('N')[0]).to eql 0xBA5EBA11
      # Fault Location
      expect(buffer[17].unpack('C')[0]).to eql 0x06
      expect(buffer[18].unpack('C')[0]).to eql 1
      expect(buffer[19].unpack('C')[0]).to eql 0x5

      hash = {}
      # decom takes just the EOF specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_eof_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CONDITION_CODE']).to eql 'CANCEL_REQUEST_RECEIVED'
      expect(hash['FILE_CHECKSUM']).to eql 0xDEADBEEF
      expect(hash['FILE_SIZE']).to eql 0xBA5EBA11
      expect(hash['TLVS'][0]['TYPE']).to eql 'ENTITY_ID'
      expect(hash['TLVS'][0]['ENTITY_ID']).to eql 5
    end
  end
end

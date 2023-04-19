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

  # Validate Table 5-7: Finished PDU Contents
  describe "build_finished_pdu" do
    it "builds a Finished PDU with no responses" do
      buffer = CfdpPdu.build_finished_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        condition_code: "NO_ERROR", # 0
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        delivery_code: "DATA_INCOMPLETE",
        file_status: "UNREPORTED",
        filestore_responses: [],
        fault_location_entity_id: nil)
      # puts buffer.formatted
      expect(buffer.length).to eql 11

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 5 # Finished per Table 5-4
      # Condition Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 0
      # Delivery Code
      expect((buffer[8].unpack('C')[0] & 0x4) >> 2).to eql 1
      # File Status
      expect(buffer[8].unpack('C')[0] & 0x3).to eql 3

      hash = {}
      # decom takes just the EOF specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_finished_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CONDITION_CODE']).to eql 'NO_ERROR'
      expect(hash['DELIVERY_CODE']).to eql 'DATA_INCOMPLETE'
      expect(hash['FILE_STATUS']).to eql 'UNREPORTED'
    end

    it "builds a Finished PDU with a filestore response" do
      buffer = CfdpPdu.build_finished_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        condition_code: "NO_ERROR", # 0
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        delivery_code: "DATA_COMPLETE",
        file_status: "FILESTORE_SUCCESS",
        filestore_responses: [{ # Table 5-17, Table 5-18
          'ACTION_CODE' => 'DELETE_FILE', # 1
          'STATUS_CODE' => 'NOT_PERFORMED', # 0xF
          'FIRST_FILE_NAME' => 'filename', # Length + value
          'SECOND_FILE_NAME' => 'test', # Length + value
          'FILESTORE_MESSAGE' => 'Message', # Length + value
        }],
        fault_location_entity_id: nil)
      # puts buffer.formatted
      expect(buffer.length).to eql 36

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 5 # Finished per Table 5-4
      # Condition Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 0
      # Delivery Code
      expect((buffer[8].unpack('C')[0] & 0x4) >> 2).to eql 0
      # File Status
      expect(buffer[8].unpack('C')[0] & 0x3).to eql 2
      # Filestore Response TLV Type (5.4.2.1)
      expect(buffer[9].unpack('C')[0]).to eql 1
      # Filestore Response TLV Length
      expect(buffer[10].unpack('C')[0]).to eql 23
      # Table 5-17: Filestore Response TLV Contents
      # Filestore Response Action Code
      expect(buffer[11].unpack('C')[0] >> 4).to eql 1
      # Filestore Response Status Code
      expect(buffer[11].unpack('C')[0] & 0xF).to eql 0xF
      # Filestore Response First File Name
      expect(buffer[12].unpack('C')[0]).to eql 8
      expect(buffer[13..20].unpack('A*')[0]).to eql 'filename'
      # Filestore Response Second File Name
      expect(buffer[21].unpack('C')[0]).to eql 4
      expect(buffer[22..25].unpack('A*')[0]).to eql 'test'
      # Filestore Response Message
      expect(buffer[26].unpack('C')[0]).to eql 7
      expect(buffer[27..33].unpack('A*')[0]).to eql 'Message'

      hash = {}
      # decom takes just the EOF specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_finished_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CONDITION_CODE']).to eql 'NO_ERROR'
      expect(hash['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
      expect(hash['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'
      tlv = hash['TLVS'][0]
      expect(tlv['TYPE']).to eql 'FILESTORE_RESPONSE'
      expect(tlv['ACTION_CODE']).to eql 'DELETE_FILE'
      expect(tlv['STATUS_CODE']).to eql 'NOT_PERFORMED'
      expect(tlv['FIRST_FILE_NAME']).to eql 'filename'
      expect(tlv['SECOND_FILE_NAME']).to eql 'test'
      expect(tlv['FILESTORE_MESSAGE']).to eql 'Message'
    end

    it "builds a Finished PDU with a fault location" do
      buffer = CfdpPdu.build_finished_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        condition_code: "INACTIVITY_DETECTED", # 8
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        delivery_code: "DATA_INCOMPLETE",
        file_status: "FILESTORE_REJECTION",
        filestore_responses: [],
        fault_location_entity_id: 1)
      # puts buffer.formatted
      expect(buffer.length).to eql 14

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 5 # Finished per Table 5-4
      # Condition Code
      expect(buffer[8].unpack('C')[0] >> 4).to eql 8
      # Delivery Code
      expect((buffer[8].unpack('C')[0] & 0x4) >> 2).to eql 1
      # File Status
      expect(buffer[8].unpack('C')[0] & 0x3).to eql 1
      # File Status
      expect(buffer[8].unpack('C')[0] & 0x3).to eql 1
      # TLV Entity Type (5.4.6)
      expect(buffer[9].unpack('C')[0]).to eql 6
      # TLV Length
      expect(buffer[10].unpack('C')[0]).to eql 1
      # Entity
      expect(buffer[11].unpack('C')[0]).to eql 1

      hash = {}
      # decom takes just the EOF specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_finished_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CONDITION_CODE']).to eql 'INACTIVITY_DETECTED'
      expect(hash['DELIVERY_CODE']).to eql 'DATA_INCOMPLETE'
      expect(hash['FILE_STATUS']).to eql 'FILESTORE_REJECTION'
      tlv = hash['TLVS'][0]
      expect(tlv['TYPE']).to eql 'ENTITY_ID'
      expect(tlv['ENTITY_ID']).to eql 1
    end
  end
end

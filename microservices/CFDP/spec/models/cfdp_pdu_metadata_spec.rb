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

  # Validate Table 5-9: Metadata PDU Contents
  describe "build_metadata_pdu" do
    it "builds a Metadata PDU with no options" do
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: 1,
        options: [])
      # puts buffer.formatted
      expect(buffer.length).to eql 29

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 7 # Metadata per Table 5-4
      # Closure requested
      expect(buffer[8].unpack('C')[0] >> 6).to eql 1
      # Checksum type
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # legacy modular checksum
      # File Size
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # Source File Name
      expect(buffer[13].unpack('C')[0]).to eql 8
      expect(buffer[14..21].unpack('A*')[0]).to eql 'filename'
      # Destination File Name
      expect(buffer[22].unpack('C')[0]).to eql 4
      expect(buffer[23..26].unpack('A*')[0]).to eql 'test'

      hash = {}
      # decom takes just the Metadata specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_metadata_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CLOSURE_REQUESTED']).to eql 'CLOSURE_REQUESTED'
      expect(hash['CHECKSUM_TYPE']).to eql 0
      expect(hash['FILE_SIZE']).to eql 0xDEADBEEF
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
    end

    it "builds a Metadata PDU with option: filestore request" do
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: 0,
        options: [{
          "TLV_TYPE" => "FILESTORE_REQUEST",
          "ACTION_CODE" => "DENY_DIRECTORY", # 8
          "FIRST_FILE_NAME" => "filename",
          "SECOND_FILE_NAME" => "test",
        }])
      # puts buffer.formatted
      expect(buffer.length).to eql 46

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 7 # Metadata per Table 5-4
      # Closure requested
      expect(buffer[8].unpack('C')[0] >> 6).to eql 0
      # Checksum type
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # legacy modular checksum
      # File Size
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # Source File Name
      expect(buffer[13].unpack('C')[0]).to eql 8
      expect(buffer[14..21].unpack('A*')[0]).to eql 'filename'
      # Destination File Name
      expect(buffer[22].unpack('C')[0]).to eql 4
      expect(buffer[23..26].unpack('A*')[0]).to eql 'test'
      # Option TLV Type
      expect(buffer[27].unpack('C')[0]).to eql 0 # 5.4.1.1 Filestore Request
      # Option TLV Length
      expect(buffer[28].unpack('C')[0]).to eql 15
      # Option TLV Value
      # Action Code
      expect(buffer[29].unpack('C')[0] >> 4).to eql 8
      # First filename length
      expect(buffer[30].unpack('C')[0]).to eql 8
      # First filename
      expect(buffer[31..38].unpack('A*')[0]).to eql 'filename'
      # Second filename length
      expect(buffer[39].unpack('C')[0]).to eql 4
      # Second filename
      expect(buffer[40..43].unpack('A*')[0]).to eql 'test'

      hash = {}
      # decom takes just the Metadata specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_metadata_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CLOSURE_REQUESTED']).to eql 'CLOSURE_NOT_REQUESTED'
      expect(hash['CHECKSUM_TYPE']).to eql 0
      expect(hash['FILE_SIZE']).to eql 0xDEADBEEF
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
      tlv = hash['TLVS'][0]
      expect(tlv['TYPE']).to eql 'FILESTORE_REQUEST'
      expect(tlv['ACTION_CODE']).to eql 'DENY_DIRECTORY'
      expect(tlv['FIRST_FILE_NAME']).to eql 'filename'
      expect(tlv['SECOND_FILE_NAME']).to eql 'test'
    end

    it "builds a Metadata PDU with option: message to user" do
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: 0,
        options: [{
          "TLV_TYPE" => "MESSAGE_TO_USER",
          "MESSAGE_TO_USER" => "Hello"
        }])
      # puts buffer.formatted
      expect(buffer.length).to eql 36

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 7 # Metadata per Table 5-4
      # Closure requested
      expect(buffer[8].unpack('C')[0] >> 6).to eql 0
      # Checksum type
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # legacy modular checksum
      # File Size
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # Source File Name
      expect(buffer[13].unpack('C')[0]).to eql 8
      expect(buffer[14..21].unpack('A*')[0]).to eql 'filename'
      # Destination File Name
      expect(buffer[22].unpack('C')[0]).to eql 4
      expect(buffer[23..26].unpack('A*')[0]).to eql 'test'
      # Option TLV Type
      expect(buffer[27].unpack('C')[0]).to eql 2 # 5.4.3 Message to User
      # Option TLV Length
      expect(buffer[28].unpack('C')[0]).to eql 5
      # Option TLV Value
      expect(buffer[29..33].unpack('A*')[0]).to eql 'Hello'

      hash = {}
      # decom takes just the Metadata specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_metadata_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CLOSURE_REQUESTED']).to eql 'CLOSURE_NOT_REQUESTED'
      expect(hash['CHECKSUM_TYPE']).to eql 0
      expect(hash['FILE_SIZE']).to eql 0xDEADBEEF
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
      tlv = hash['TLVS'][0]
      expect(tlv['TYPE']).to eql 'MESSAGE_TO_USER'
      expect(tlv['MESSAGE_TO_USER']).to eql 'Hello'
    end

    it "builds a Metadata PDU with option: fault handler override" do
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: 0,
        options: [{
          "TLV_TYPE" => "FAULT_HANDLER_OVERRIDE",
          "CONDITION_CODE" => "INVALID_TRANSMISSION_MODE", # 3
          "HANDLER_CODE" => "ABONDON_TRANSACTION", # 4
        }])
      # puts buffer.formatted
      expect(buffer.length).to eql 32

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 7 # Metadata per Table 5-4
      # Closure requested
      expect(buffer[8].unpack('C')[0] >> 6).to eql 0
      # Checksum type
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # legacy modular checksum
      # File Size
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # Source File Name
      expect(buffer[13].unpack('C')[0]).to eql 8
      expect(buffer[14..21].unpack('A*')[0]).to eql 'filename'
      # Destination File Name
      expect(buffer[22].unpack('C')[0]).to eql 4
      expect(buffer[23..26].unpack('A*')[0]).to eql 'test'
      # Option TLV Type
      expect(buffer[27].unpack('C')[0]).to eql 4 # 5.4.4 Fault Handler Override
      # Option TLV Length
      expect(buffer[28].unpack('C')[0]).to eql 1
      # Option TLV Value
      expect(buffer[29].unpack('C')[0] >> 4).to eql 3
      expect(buffer[29].unpack('C')[0] & 0xF).to eql 4

      hash = {}
      # decom takes just the Metadata specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_metadata_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CLOSURE_REQUESTED']).to eql 'CLOSURE_NOT_REQUESTED'
      expect(hash['CHECKSUM_TYPE']).to eql 0
      expect(hash['FILE_SIZE']).to eql 0xDEADBEEF
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
      tlv = hash['TLVS'][0]
      expect(tlv['TYPE']).to eql 'FAULT_HANDLER_OVERRIDE'
      expect(tlv['CONDITION_CODE']).to eql 'INVALID_TRANSMISSION_MODE'
      expect(tlv['HANDLER_CODE']).to eql 'ABONDON_TRANSACTION'
    end

    it "builds a Metadata PDU with option: flow label" do
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: 0,
        options: [{
          "TLV_TYPE" => "FLOW_LABEL",
          "FLOW_LABEL" => "flow"
        }])
      # puts buffer.formatted
      expect(buffer.length).to eql 35

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 7 # Metadata per Table 5-4
      # Closure requested
      expect(buffer[8].unpack('C')[0] >> 6).to eql 0
      # Checksum type
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # legacy modular checksum
      # File Size
      expect(buffer[9..12].unpack('N')[0]).to eql 0xDEADBEEF
      # Source File Name
      expect(buffer[13].unpack('C')[0]).to eql 8
      expect(buffer[14..21].unpack('A*')[0]).to eql 'filename'
      # Destination File Name
      expect(buffer[22].unpack('C')[0]).to eql 4
      expect(buffer[23..26].unpack('A*')[0]).to eql 'test'
      # Option TLV Type
      expect(buffer[27].unpack('C')[0]).to eql 5 # 5.4.5 Flow Label
      # Option TLV Length
      expect(buffer[28].unpack('C')[0]).to eql 4
      # Option TLV Value
      expect(buffer[29..32].unpack('A*')[0]).to eql 'flow'

      hash = {}
      # decom takes just the Metadata specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_metadata_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CLOSURE_REQUESTED']).to eql 'CLOSURE_NOT_REQUESTED'
      expect(hash['CHECKSUM_TYPE']).to eql 0
      expect(hash['FILE_SIZE']).to eql 0xDEADBEEF
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
      tlv = hash['TLVS'][0]
      expect(tlv['TYPE']).to eql 'FLOW_LABEL'
      expect(tlv['FLOW_LABEL']).to eql 'flow'
    end
  end
end
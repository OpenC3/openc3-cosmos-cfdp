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
      expect(buffer[1..2].unpack('n')[0]).to eql 22 # PDU_DATA_LENGTH - Directive Code plus Data plus CRC

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
      hash['VERSION'] = 1
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
          "TYPE" => "FILESTORE_REQUEST",
          "ACTION_CODE" => "DELETE_FILE", # 1
          "FIRST_FILE_NAME" => "first",
        },
        {
          "TYPE" => "FILESTORE_REQUEST",
          "ACTION_CODE" => "RENAME_FILE", # 2
          "FIRST_FILE_NAME" => "begin",
          "SECOND_FILE_NAME" => "end",
        }])
      # puts buffer.formatted
      expect(buffer.length).to eql 51

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
      expect(buffer[28].unpack('C')[0]).to eql 7 # Action code + length field + length
      # Option TLV Value
      # Action Code
      expect(buffer[29].unpack('C')[0] >> 4).to eql 1
      # First filename length
      expect(buffer[30].unpack('C')[0]).to eql 5
      # First filename
      expect(buffer[31..35].unpack('A*')[0]).to eql 'first'
      # Option TLV Type
      expect(buffer[36].unpack('C')[0]).to eql 0 # 5.4.1.1 Filestore Request
      # Option TLV Length
      expect(buffer[37].unpack('C')[0]).to eql 11
      # Option TLV Value
      # Action Code
      expect(buffer[38].unpack('C')[0] >> 4).to eql 2
      # First filename length
      expect(buffer[39].unpack('C')[0]).to eql 5
      # First filename
      expect(buffer[40..44].unpack('A*')[0]).to eql 'begin'
      # Second filename length
      expect(buffer[45].unpack('C')[0]).to eql 3
      # Second filename
      expect(buffer[46..48].unpack('A*')[0]).to eql 'end'

      hash = {}
      hash['VERSION'] = 1
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
      expect(tlv['ACTION_CODE']).to eql 'DELETE_FILE'
      expect(tlv['FIRST_FILE_NAME']).to eql 'first'
      tlv = hash['TLVS'][1]
      expect(tlv['TYPE']).to eql 'FILESTORE_REQUEST'
      expect(tlv['ACTION_CODE']).to eql 'RENAME_FILE'
      expect(tlv['FIRST_FILE_NAME']).to eql 'begin'
      expect(tlv['SECOND_FILE_NAME']).to eql 'end'
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
          "TYPE" => "MESSAGE_TO_USER",
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
      hash['VERSION'] = 1
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
          "TYPE" => "FAULT_HANDLER_OVERRIDE",
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
      hash['VERSION'] = 1
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
          "TYPE" => "FLOW_LABEL",
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
      hash['VERSION'] = 1
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

    it "builds a Metadata PDU for version 0" do
      destination_entity = CfdpMib.entity(@destination_entity_id)
      destination_entity['protocol_version_number'] = 0
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: destination_entity,
        file_size: 0xDEADBEEF,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: 0,
        options: [])
      # puts buffer.formatted
      expect(buffer.length).to eql 29

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number
      expect(buffer[1..2].unpack('n')[0]).to eql 22 # PDU_DATA_LENGTH - Directive Code plus Data plus CRC

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

      hash = {}
      hash['VERSION'] = 0
      # decom takes just the Metadata specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_metadata_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[8..-3])
      expect(hash['CLOSURE_REQUESTED']).to eql nil
      expect(hash['CHECKSUM_TYPE']).to eql nil
      expect(hash['FILE_SIZE']).to eql 0xDEADBEEF
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
    end

    it "builds a Metadata PDU with unknown checksum type, large file size, and default closure requested" do
      destination_entity = CfdpMib.entity(@destination_entity_id)
      destination_entity['default_checksum_type'] = 9 # Unsupported
      buffer = CfdpPdu.build_metadata_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: destination_entity,
        file_size: 0x100000000,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        source_file_name: "filename",
        destination_file_name: "test",
        closure_requested: nil,
        options: [])
      # puts buffer.formatted
      expect(buffer.length).to eql 33

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number
      expect(buffer[1..2].unpack('n')[0]).to eql 26 # PDU_DATA_LENGTH - Directive Code plus Data plus CRC

      # Directive Code
      expect(buffer[7].unpack('C')[0]).to eql 7 # Metadata per Table 5-4
      # Closure requested
      expect(buffer[8].unpack('C')[0] >> 6).to eql 1
      # Checksum type
      expect(buffer[8].unpack('C')[0] & 0xF).to eql 0 # legacy modular checksum
      # File Size
      expect(buffer[9..16].unpack('Q>')[0]).to eql 0x100000000
      # Source File Name
      expect(buffer[17].unpack('C')[0]).to eql 8
      expect(buffer[18..25].unpack('A*')[0]).to eql 'filename'
      # Destination File Name
      expect(buffer[26].unpack('C')[0]).to eql 4
      expect(buffer[27..30].unpack('A*')[0]).to eql 'test'

      # Test with toplevel decom
      hash = CfdpPdu.decom(buffer)
      expect(hash['CLOSURE_REQUESTED']).to eql "CLOSURE_REQUESTED"
      expect(hash['CHECKSUM_TYPE']).to eql 0
      expect(hash['FILE_SIZE']).to eql 0x100000000
      expect(hash['SOURCE_FILE_NAME']).to eql 'filename'
      expect(hash['DESTINATION_FILE_NAME']).to eql 'test'
    end
  end
end

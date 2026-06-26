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

  # Validate Table 5-14: File Data PDU Contents
  describe "build_file_data_pdu" do
    it "builds a Prompt PDU with nak response" do
      buffer = CfdpPdu.build_file_data_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        offset: 0xDEADBEEF,
        file_data: [0xAA, 0x55].pack("C*"), # raw file data
        record_continuation_state: 'START_AND_END',
        segment_metadata: nil)
      # puts buffer.formatted
      expect(buffer.length).to eql 15

      # By default the first 7 bytes are the header
      # This assumes 1 byte per entity ID and sequence number
      expect(buffer[1..2].unpack('n')[0]).to eql 8 # PDU_DATA_LENGTH - Directive Code plus Data plus CRC

      # PDU Type
      expect((buffer[0].unpack('C')[0] >> 4) & 0x1).to eql 1 # File Data per Table 5-1
      # Offset
      expect(buffer[7..10].unpack('N')[0]).to eql 0xDEADBEEF
      expect(buffer[11].unpack('C')[0]).to eql 0xAA
      expect(buffer[12].unpack('C')[0]).to eql 0x55

      hash = {}
      # decom takes just the Prompt specific part of the buffer
      # so start at offset 8 and ignore the 2 checksum bytes
      CfdpPdu.decom_file_data_pdu_contents(CfdpPdu.new(crcs_required: false), hash, buffer[7..-3])
      expect(hash['OFFSET']).to eql 0xDEADBEEF
      # TODO: FILE_DATA doesn't round trip
      # expect(hash['FILE_DATA']).to eql [0xAA, 0x55].pack("C*")
    end

    # The build path always writes SEGMENT_METADATA_FLAG = NOT_PRESENT ("not
    # implemented"), but a remote entity can send segment metadata, so the
    # decom path must handle it. Build the PDU object, flip the flag PRESENT,
    # and round-trip through build/decom of the file data contents.
    it "round trips segment metadata when the flag is PRESENT" do
      pdu = CfdpPdu.build_initial_pdu(
        type: "FILE_DATA",
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil)
      pdu.write("SEGMENT_METADATA_FLAG", "PRESENT")

      contents = pdu.build_file_data_pdu_contents(
        offset: 0x1234,
        file_data: [0xAA, 0x55].pack("C*"),
        record_continuation_state: 'START_AND_END',
        segment_metadata: "META")

      hash = {}
      CfdpPdu.decom_file_data_pdu_contents(pdu, hash, contents)
      expect(hash['RECORD_CONTINUATION_STATE']).to eql 'START_AND_END'
      expect(hash['SEGMENT_METADATA_LENGTH']).to eql 4
      expect(hash['SEGMENT_METADATA']).to eql "META"
      expect(hash['OFFSET']).to eql 0x1234
    end

    it "uses a 64-bit offset for large files" do
      # file_size above the 32-bit boundary forces LARGE_FILE_FLAG != SMALL_FILE
      buffer = CfdpPdu.build_file_data_pdu(
        source_entity: CfdpMib.entity(@source_entity_id),
        transaction_seq_num: 1,
        destination_entity: CfdpMib.entity(@destination_entity_id),
        file_size: 0x1_0000_0000,
        segmentation_control: "NOT_PRESERVED",
        transmission_mode: nil,
        offset: 0xDEADBEEF,
        file_data: [0xAA, 0x55].pack("C*"),
        record_continuation_state: nil,
        segment_metadata: nil)

      # Large files use a 64-bit (8 byte) offset field instead of 32-bit.
      # Header is 7 bytes by default, so the offset occupies bytes 7..14.
      expect(buffer[7..14].unpack('Q>')[0]).to eql 0xDEADBEEF
      expect(buffer[15].unpack('C')[0]).to eql 0xAA
      expect(buffer[16].unpack('C')[0]).to eql 0x55
    end
  end
end

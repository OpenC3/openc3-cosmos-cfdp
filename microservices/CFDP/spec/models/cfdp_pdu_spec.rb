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
require 'openc3/models/microservice_model'
require 'openc3/utilities/store_autoload'

RSpec.describe CfdpPdu, type: :model do
  # Validate Table 5-1: Fixed PDU Header Fields
  describe "initialize" do
    it "builds a PDU with crcs" do
      pdu = CfdpPdu.new(crcs_required: true)
      expect(pdu.items.keys).to include("CRC")
      expect(pdu.buffer.length).to eql 6
    end

    it "builds a PDU without crcs" do
      pdu = CfdpPdu.new(crcs_required: false)
      expect(pdu.items.keys).to_not include("CRC")
      expect(pdu.buffer.length).to eql 4
    end

  end

  # CfdpPdu.build clones a shared prototype instead of redefining the items on every PDU.
  # Structure#clone shares the item definitions, so these guard the invariant that makes that safe.
  describe "build" do
    it "produces a PDU equivalent to new" do
      [true, false].each do |crcs_required|
        built = CfdpPdu.build(crcs_required: crcs_required)
        fresh = CfdpPdu.new(crcs_required: crcs_required)
        expect(built.items.keys).to eql fresh.items.keys
        expect(built.buffer).to eql fresh.buffer
      end
    end

    it "returns independent PDUs that do not alias each other" do
      first = CfdpPdu.build(crcs_required: false)
      second = CfdpPdu.build(crcs_required: false)

      first.write("VERSION", 1)
      first.write("VARIABLE_DATA", "\x01" * 10)
      second.write("VERSION", 0)
      second.write("VARIABLE_DATA", "\x02" * 400)

      expect(first.read("VERSION")).to eql 1
      expect(first.read("VARIABLE_DATA")).to eql "\x01" * 10
      expect(second.read("VERSION")).to eql 0
      expect(second.read("VARIABLE_DATA")).to eql "\x02" * 400
    end

    it "does not let a write leak into later PDUs" do
      CfdpPdu.build(crcs_required: false).write("VARIABLE_DATA", "\xFF" * 500)
      expect(CfdpPdu.build(crcs_required: false).read("VARIABLE_DATA")).to eql ""
      expect(CfdpPdu.build(crcs_required: false).buffer).to eql CfdpPdu.new(crcs_required: false).buffer
    end

    it "stays independent when built concurrently from many threads" do
      mismatches = Queue.new
      threads = 8.times.map do |t|
        Thread.new do
          200.times do |i|
            pdu = CfdpPdu.build(crcs_required: false)
            payload = "#{t}-#{i}".b * 3
            pdu.write("VARIABLE_DATA", payload)
            mismatches << [t, i] if pdu.read("VARIABLE_DATA") != payload
          end
        end
      end
      threads.each(&:join)
      expect(mismatches.size).to eql 0
    end
  end

  describe "initialize" do
    it "sets the version field" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.version = 1
      expect(pdu.buffer[0].unpack('C')[0] >> 5).to eql 1
    end

    it "sets the PDU type" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.type = 1
      expect(pdu.buffer[0].unpack('C')[0] >> 4).to eql 1
    end

    it "sets the direction" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.direction = 1
      expect(pdu.buffer[0].unpack('C')[0] >> 3).to eql 1
    end

    it "sets the transmission mode" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.transmission_mode = 1
      expect(pdu.buffer[0].unpack('C')[0] >> 2).to eql 1
    end

    it "sets the crc flag" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.crc_flag = 1
      expect(pdu.buffer[0].unpack('C')[0] >> 1).to eql 1
    end

    it "sets the large file flag" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.large_file_flag = 1
      expect(pdu.buffer[0].unpack('C')[0]).to eql 1
    end

    it "sets the length" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.pdu_data_length = 0x1234
      expect(pdu.buffer[1..2].unpack('n')[0]).to eql 0x1234
    end

    it "sets the segmentation control" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.segmentation_control = 1
      expect(pdu.buffer[3].unpack('C')[0] >> 7).to eql 1
    end

    it "sets the entity id length" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.entity_id_length = 7
      expect(pdu.buffer[3].unpack('C')[0] >> 4).to eql 7
    end

    it "sets the segment metadata flag" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.segment_metadata_flag = 1
      expect(pdu.buffer[3].unpack('C')[0] >> 3).to eql 1
    end

    it "sets the sequence number length" do
      pdu = CfdpPdu.new(crcs_required: false)
      pdu.enable_method_missing
      pdu.sequence_number_length = 7
      expect(pdu.buffer[3].unpack('C')[0]).to eql 7
    end
  end

  # A receiver replies to the file sender with ACK, NAK, Keep Alive and Finished PDUs. The header ids
  # still name the sender as source and the receiver as destination, so remote_entity is what selects
  # the peer whose protocol version, CRC, and length settings must be honored.
  describe "reply PDUs built with remote_entity" do
    before(:each) do
      mock_redis()
      ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
      @local_entity_id = 1
      @remote_entity_id = 2
    end

    def setup_mib(local_options, remote_options)
      options = [["source_entity_id", @local_entity_id]]
      options.concat(local_options)
      options << ["destination_entity_id", @remote_entity_id]
      options.concat(remote_options)
      options << ["root_path", SPEC_DIR]
      model = OpenC3::MicroserviceModel.new(name: ENV['OPENC3_MICROSERVICE_NAME'], scope: "DEFAULT", options: options)
      model.create
      CfdpMib.setup
    end

    def build_reply(type, local, remote)
      common = { source_entity: remote, transaction_seq_num: 1, destination_entity: local, remote_entity: remote, transmission_mode: "ACKNOWLEDGED" }
      case type
      when :ack
        CfdpPdu.build_ack_pdu(**common, condition_code: "NO_ERROR", ack_directive_code: "EOF", transaction_status: "ACTIVE")
      when :nak
        CfdpPdu.build_nak_pdu(**common, file_size: 100, start_of_scope: 0, end_of_scope: 100, segment_requests: [[0, 100]])
      when :keep_alive
        CfdpPdu.build_keep_alive_pdu(**common, file_size: 100, progress: 50)
      when :finished
        CfdpPdu.build_finished_pdu(**common, condition_code: "NO_ERROR", delivery_code: "DATA_COMPLETE", file_status: "FILESTORE_SUCCESS")
      end
    end

    def check_header(buffer, version:, crc:, entity_id_length:, sequence_number_length:)
      expect(buffer[0].unpack('C')[0] >> 5).to eql version
      expect((buffer[0].unpack('C')[0] >> 1) & 1).to eql(crc ? 1 : 0)
      expect((buffer[3].unpack('C')[0] >> 4) & 7).to eql entity_id_length
      expect(buffer[3].unpack('C')[0] & 7).to eql sequence_number_length
      # PDU_DATA_LENGTH covers everything after the header, including the CRC when present
      header_length = 4 + (2 * (entity_id_length + 1)) + (sequence_number_length + 1)
      expect(buffer[1..2].unpack('n')[0]).to eql(buffer.length - header_length)
      expect(CfdpPdu::CRC16.calc(buffer[0..-3])).to eql(buffer[-2..-1].unpack('n')[0]) if crc
    end

    [:ack, :nak, :keep_alive, :finished].each do |type|
      it "uses the remote entity's version, CRC, and lengths for #{type}" do
        setup_mib(
          [["crcs_required", "false"], ["protocol_version_number", "1"], ["entity_id_length", "1"], ["sequence_number_length", "2"]],
          [["crcs_required", "true"], ["protocol_version_number", "0"], ["entity_id_length", "0"], ["sequence_number_length", "0"]])
        buffer = build_reply(type, CfdpMib.entity(@local_entity_id), CfdpMib.entity(@remote_entity_id))
        check_header(buffer, version: 0, crc: true, entity_id_length: 0, sequence_number_length: 0)

        # The local entity accepts the reply because the CRC flag in the header says a CRC is present
        hash = CfdpPdu.decom(buffer)
        expect(hash['VERSION']).to eql 0
        expect(hash['CRC_FLAG']).to eql 'CRC_PRESENT'
        expect(hash['SOURCE_ENTITY_ID']).to eql @remote_entity_id
        expect(hash['DESTINATION_ENTITY_ID']).to eql @local_entity_id
        expect(hash['END_SYSTEM_STATUS']).to eql 1 if type == :finished # Version 0 Finished PDU field
      end

      it "omits the CRC for #{type} when the remote entity does not require one" do
        setup_mib(
          [["crcs_required", "true"], ["protocol_version_number", "0"], ["entity_id_length", "0"], ["sequence_number_length", "0"]],
          [["crcs_required", "false"], ["protocol_version_number", "1"], ["entity_id_length", "1"], ["sequence_number_length", "1"]])
        buffer = build_reply(type, CfdpMib.entity(@local_entity_id), CfdpMib.entity(@remote_entity_id))
        check_header(buffer, version: 1, crc: false, entity_id_length: 1, sequence_number_length: 1)
      end
    end
  end
end

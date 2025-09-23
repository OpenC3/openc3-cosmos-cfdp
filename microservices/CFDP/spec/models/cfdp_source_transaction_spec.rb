# encoding: ascii-8bit

# Copyright 2025 OpenC3, Inc.
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

require 'rails_helper'

RSpec.describe CfdpSourceTransaction do
  before(:each) do
    # Mock CfdpTopic
    allow(CfdpTopic).to receive(:write_indication)

    ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
    ENV['OPENC3_SCOPE'] = 'DEFAULT'

    allow(OpenC3::Logger).to receive(:info)

    @transactions = {}
    allow(CfdpMib).to receive(:transactions).and_return(@transactions)

    @source_entity = {
      'id' => 1,
      'name' => 'SOURCE',
      'protocol_version' => 1,
      'fault_handler' => {
        'NO_ERROR' => 'IGNORE_ERROR',
        'FILESTORE_REJECTION' => 'ISSUE_NOTICE_OF_CANCELLATION',
        'FILE_CHECKSUM_FAILURE' => 'ISSUE_NOTICE_OF_SUSPENSION',
        'FILE_SIZE_ERROR' => 'ABANDON_TRANSACTION',
        'CHECK_LIMIT_REACHED' => 'IGNORE_ERROR'
      }
    }

    allow(CfdpMib).to receive(:source_entity).and_return(@source_entity)

    # Mock CfdpModel for sequence numbers
    allow(CfdpModel).to receive(:get_next_transaction_seq_num).and_return(123)
  end

  describe "initialize" do
    it "initializes with default source entity" do
      source_transaction = CfdpSourceTransaction.new

      expect(source_transaction.id).to eq("1__123")
      expect(source_transaction.instance_variable_get(:@source_entity)).to eq(@source_entity)
      expect(source_transaction.instance_variable_get(:@transaction_seq_num)).to eq(123)
    end

    it "initializes with explicit source entity" do
      custom_entity = @source_entity.merge('id' => 2)

      source_transaction = CfdpSourceTransaction.new(source_entity: custom_entity)

      expect(source_transaction.id).to eq("2__123")
      expect(source_transaction.instance_variable_get(:@source_entity)).to eq(custom_entity)
    end

    it "adds itself to CfdpMib transactions hash during initialization" do
      # Clear transactions hash first
      @transactions.clear

      source_transaction = CfdpSourceTransaction.new

      # Verify the transaction was added to the MIB transactions hash
      expect(@transactions["1__123"]).to eq(source_transaction)
      expect(@transactions).to have_key("1__123")
    end

    it "raises error if no source entity is defined" do
      allow(CfdpMib).to receive(:source_entity).and_return(nil)

      expect {
        CfdpSourceTransaction.new
      }.to raise_error("No source entity defined")
    end

    it "initializes with proper default values" do
      source_transaction = CfdpSourceTransaction.new

      expect(source_transaction.instance_variable_get(:@finished_pdu_hash)).to be_nil
      expect(source_transaction.instance_variable_get(:@destination_entity)).to be_nil
      expect(source_transaction.instance_variable_get(:@eof_count)).to eq(0)
      expect(source_transaction.filestore_responses).to eq([])
      expect(source_transaction.instance_variable_get(:@metadata_pdu_hash)).to eq({})
      expect(source_transaction.copy_state).to be_nil
    end
  end

  describe "save_state optimization during file PDU transmission" do
    before(:each) do
      mock_redis()

      @destination_entity = {
        'id' => 2,
        'name' => 'DESTINATION',
        'maximum_file_segment_length' => 100,
        'default_transmission_mode' => 'UNACKNOWLEDGED',
        'default_checksum_type' => 'NULL',
        'cmd_info' => ['TGT', 'PKT', 'ITEM']
      }
      allow(CfdpMib).to receive(:entity).with(2).and_return(@destination_entity)
      allow(CfdpMib).to receive(:get_source_file).and_return(StringIO.new("A" * 15000))
      allow(CfdpMib).to receive(:complete_source_file)
      allow(CfdpPdu).to receive(:build_file_data_pdu).and_return("mock_pdu")

      @source_transaction = CfdpSourceTransaction.new
      @source_transaction.instance_variable_set(:@destination_entity, @destination_entity)
      @source_transaction.instance_variable_set(:@source_file_name, "test.txt")
      @source_transaction.instance_variable_set(:@destination_file_name, "test.txt")
      @source_transaction.instance_variable_set(:@file_size, 15000)
      @source_transaction.instance_variable_set(:@read_size, 100)
      @source_transaction.instance_variable_set(:@segmentation_control, "NOT_PRESERVED")
      @source_transaction.instance_variable_set(:@transmission_mode, "UNACKNOWLEDGED")
      @source_transaction.instance_variable_set(:@target_name, "TGT")
      @source_transaction.instance_variable_set(:@packet_name, "PKT")
      @source_transaction.instance_variable_set(:@item_name, "ITEM")
      checksum_mock = instance_double("CfdpChecksum", add: nil)
      @source_transaction.instance_variable_set(:@file_checksum, checksum_mock)

      allow(@source_transaction).to receive(:cfdp_cmd)
    end

    it "only calls save_state every 100 PDUs during file transmission" do
      allow(@source_transaction).to receive(:save_state)

      150.times do |i|
        @source_transaction.send(:copy_file_send_file_data_pdu,
          destination_entity_id: 2,
          fault_handler_overrides: [],
          transmission_mode: "UNACKNOWLEDGED",
          closure_requested: nil,
          messages_to_user: [],
          filestore_requests: [])
      end

      expect(@source_transaction).to have_received(:save_state).exactly(1).times
    end
  end
end

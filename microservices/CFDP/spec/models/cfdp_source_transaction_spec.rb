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

require 'timeout'

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
      @source_transaction.instance_variable_set(:@file_checksum, CfdpChecksum.new(100))

      allow(@source_transaction).to receive(:cfdp_cmd)
    end

    it "only calls save_state every 100 PDUs during file transmission" do
      allow(@source_transaction).to receive(:save_state)

      150.times do |i|
        @source_transaction.send(:copy_file_send_file_data_pdu,
          transaction_seq_num: 123,
          transaction_id: "1__123",
          destination_entity_id: 2,
          source_file_name: "test.txt",
          destination_file_name: "test.txt",
          fault_handler_overrides: [],
          transmission_mode: "UNACKNOWLEDGED",
          closure_requested: nil,
          messages_to_user: [],
          filestore_requests: [])
      end

      expect(@source_transaction).to have_received(:save_state).exactly(1).times
    end

    it "calls save_state when destination file name is missing" do
      @source_transaction.copy_file_send_file_data_pdu(
        transaction_seq_num: 123,
        transaction_id: "1__123",
        destination_entity_id: 2,
        source_file_name: "test.txt",
        destination_file_name: nil,
        fault_handler_overrides: [],
        transmission_mode: "UNACKNOWLEDGED",
        closure_requested: nil,
        messages_to_user: [],
        filestore_requests: [])

      state = @source_transaction.load_state("1__123")
      # Spot check some state to ensure it all round trips
      expect(state["source_file_name"]).to eq("test.txt")
      expect(state["destination_file_name"]).to eq("test.txt")
      expect(state["source_entity"]["id"]).to eq(1)
      expect(state["source_entity"]["name"]).to eq("SOURCE")
      expect(state["destination_entity"]["id"]).to eq(2)
      expect(state["destination_entity"]["name"]).to eq("DESTINATION")
      expect(state["destination_entity"]["cmd_info"]).to eq(["TGT", "PKT", "ITEM"])
      # CfdpChecksum round trips as a class
      expect(state["file_checksum"]).to be_a CfdpChecksum
      expect(state["file_checksum"].checksum(false, false)).to eq(100)
    end

    it "opens the source file once for the whole transfer" do
      allow(@source_transaction).to receive(:save_state)
      # CfdpMib.get_source_file downloads the entire object when a bucket is configured, so
      # opening it per PDU made a transfer cost O(n^2) in file size
      args = {
        transaction_seq_num: 123, transaction_id: "1__123", destination_entity_id: 2,
        source_file_name: "test.txt", destination_file_name: "test.txt",
        fault_handler_overrides: [], transmission_mode: "UNACKNOWLEDGED",
        closure_requested: nil, messages_to_user: [], filestore_requests: []
      }
      50.times { @source_transaction.copy_file_send_file_data_pdu(**args) }

      expect(CfdpMib).to have_received(:get_source_file).exactly(1).times
      expect(@source_transaction.instance_variable_get(:@file_offset)).to eq(50 * 100)
    end

    it "sends every segment of a StringIO source" do
      # A StringIO source is a directory listing or a transaction report. It used to be closed
      # after the first segment, so the next call saw a closed file and skipped to EOF, silently
      # truncating anything longer than maximum_file_segment_length.
      allow(@source_transaction).to receive(:save_state)
      # The outer stub makes complete_source_file a no op, which hides the close that caused the
      # truncation. Close for real so this exercises the actual behavior.
      allow(CfdpMib).to receive(:complete_source_file) { |file| file.close if file and not file.closed? }
      payload = "B" * 450 # 5 segments at 100 bytes
      io = StringIO.new(payload)
      @source_transaction.instance_variable_set(:@source_file_name, io)
      @source_transaction.instance_variable_set(:@file_size, payload.length)

      args = {
        transaction_seq_num: 123, transaction_id: "1__123", destination_entity_id: 2,
        source_file_name: io, destination_file_name: "listing.txt",
        fault_handler_overrides: [], transmission_mode: "UNACKNOWLEDGED",
        closure_requested: nil, messages_to_user: [], filestore_requests: []
      }
      10.times do
        break unless @source_transaction.copy_state == "send_file_data_pdu" || @source_transaction.copy_state.nil?
        @source_transaction.copy_file_send_file_data_pdu(**args)
      end

      expect(@source_transaction.instance_variable_get(:@file_offset)).to eq(payload.length)
      expect(@source_transaction.copy_state).to eq("send_eof_pdu")
    end

    it "closes the source file only once the transfer is over" do
      allow(@source_transaction).to receive(:save_state)
      args = {
        transaction_seq_num: 123, transaction_id: "1__123", destination_entity_id: 2,
        source_file_name: "test.txt", destination_file_name: "test.txt",
        fault_handler_overrides: [], transmission_mode: "UNACKNOWLEDGED",
        closure_requested: nil, messages_to_user: [], filestore_requests: []
      }
      5.times { @source_transaction.copy_file_send_file_data_pdu(**args) }
      expect(CfdpMib).to_not have_received(:complete_source_file)

      @source_transaction.close_source_file
      expect(CfdpMib).to have_received(:complete_source_file).exactly(1).times
      expect(@source_transaction.instance_variable_get(:@source_file)).to be_nil
    end
  end

  describe "handle_suspend" do
    before(:each) do
      mock_redis()
      allow(CfdpTopic).to receive(:write_indication)
      @source_entity['keep_alive_interval'] = 600 # resume rearms the inactivity timer
      @transaction = CfdpSourceTransaction.new
      allow(@transaction).to receive(:save_state)
    end

    it "returns immediately when the transaction is neither suspended nor frozen" do
      expect { Timeout.timeout(5) { @transaction.handle_suspend } }.to_not raise_error
    end

    it "wakes as soon as the transaction is resumed rather than polling" do
      @transaction.suspend
      expect(@transaction.state).to eq("SUSPENDED")

      woke = nil
      waiter = Thread.new do
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @transaction.handle_suspend
        woke = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      end
      sleep(0.05) # let the waiter reach the wait
      @transaction.resume

      expect(waiter.join(5)).to_not be_nil
      # The backstop timeout is 1 second, so anything well under that proves it was signalled
      expect(woke).to be < 0.5
    end

    it "wakes when the transaction is unfrozen" do
      @transaction.freeze
      waiter = Thread.new { @transaction.handle_suspend }
      sleep(0.05)
      @transaction.unfreeze
      expect(waiter.join(5)).to_not be_nil
    end

    it "wakes when the transaction is abandoned" do
      @transaction.suspend
      waiter = Thread.new { @transaction.handle_suspend }
      sleep(0.05)
      @transaction.abandon
      expect(waiter.join(5)).to_not be_nil
      expect(@transaction.state).to eq("ABANDONED")
    end
  end

  # Shutdown is cooperative so the thread running the transfer can unwind on its own rather than
  # being killed at an arbitrary point, which could leave a half written state or a partial PDU
  describe "request_shutdown" do
    before(:each) do
      mock_redis()
      allow(CfdpTopic).to receive(:write_indication)
      @transaction = CfdpSourceTransaction.new
      allow(@transaction).to receive(:save_state)
    end

    it "wakes a suspended transfer" do
      @transaction.suspend
      waiter = Thread.new { @transaction.handle_suspend }
      sleep(0.05) # let the waiter reach the wait
      @transaction.request_shutdown

      expect(waiter.join(5)).to_not be_nil
      # Still suspended rather than terminated so it resumes when the microservice comes back
      expect(@transaction.state).to eq("SUSPENDED")
    end

    it "stops the copy loop without advancing the copy state" do
      @transaction.instance_variable_set(:@copy_state, "setup")
      @transaction.request_shutdown
      expect(@transaction).to_not receive(:copy_file_setup_and_send_metadata)

      Timeout.timeout(5) do
        @transaction.copy_file_state_machine(
          destination_entity_id: 2, fault_handler_overrides: [], flow_label: nil,
          transmission_mode: "UNACKNOWLEDGED", closure_requested: nil,
          messages_to_user: [], filestore_requests: [])
      end

      expect(@transaction.copy_state).to eq("setup")
    end

    it "stops waiting for the Finished PDU without completing the transaction" do
      @source_entity['check_interval'] = 600
      @transaction.instance_variable_set(:@destination_entity, { 'enable_finished' => true })
      @transaction.instance_variable_set(:@transmission_mode, "ACKNOWLEDGED")
      @transaction.instance_variable_set(:@copy_state, "cleanup")
      @transaction.request_shutdown

      Timeout.timeout(5) do
        @transaction.copy_file_cleanup(
          transaction_seq_num: 123, transaction_id: @transaction.id, destination_entity_id: 2,
          source_file_name: nil, destination_file_name: nil, fault_handler_overrides: [],
          transmission_mode: "ACKNOWLEDGED", closure_requested: "CLOSURE_REQUESTED",
          messages_to_user: [], filestore_requests: [])
      end

      # Left in cleanup so the wait for the Finished PDU restarts when the transfer resumes
      expect(@transaction.copy_state).to eq("cleanup")
      expect(@transaction.condition_code).to eq("NO_ERROR")
    end

    it "does not shorten the inter command delay unless shutting down" do
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @transaction.cmd_delay(0.2)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.2

      @transaction.request_shutdown
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @transaction.cmd_delay(5)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
    end
  end
end

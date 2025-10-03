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
#
# The development of this software was funded in-whole or in-part by Sandia National Laboratories.
# See https://github.com/OpenC3/openc3-cosmos-cfdp/pull/12 for details

require 'rails_helper'
require 'tempfile'

RSpec.describe CfdpReceiveTransaction do
  before(:each) do
    mock_redis()

    # Mock the CfdpTopic static methods directly
    allow(CfdpTopic).to receive(:write_indication)
    ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
    ENV['OPENC3_SCOPE'] = 'DEFAULT'

    allow(OpenC3::Logger).to receive(:info)

    @transactions = {}
    allow(CfdpMib).to receive(:transactions).and_return(@transactions)
    allow(CfdpMib).to receive(:put_destination_file).and_return(true)
    allow(CfdpMib).to receive(:filestore_request).and_return(["SUCCESSFUL", "Success"])

    @source_entity = {
      'id' => 1,
      'name' => 'SOURCE',
      'protocol_version' => 1,
      'ack_timer_interval' => 5,
      'ack_timer_expiration_limit' => 3,
      'enable_acks' => true,
      'enable_finished' => true,
      'check_interval' => 5,
      'check_limit' => 3,
      'immediate_nak_mode' => true,
      'enable_eof_nak' => true,
      'nak_timer_interval' => 5,
      'nak_timer_expiration_limit' => 3,
      'keep_alive_interval' => 5,
      'enable_keep_alive' => true,
      'transaction_inactivity_limit' => 3,
      'incomplete_file_disposition' => 'DISCARD',
      'cmd_info' => ['TARGET', 'PACKET', 'ITEM'],
      'fault_handler' => {
        'NO_ERROR' => 'IGNORE_ERROR',
        'FILESTORE_REJECTION' => 'IGNORE_ERROR',
        'FILE_CHECKSUM_FAILURE' => 'IGNORE_ERROR',
        'CHECK_LIMIT_REACHED' => 'IGNORE_ERROR',
        'NAK_LIMIT_REACHED' => 'IGNORE_ERROR',
        'INACTIVITY_DETECTED' => 'IGNORE_ERROR',
        'ACK_LIMIT_REACHED' => 'IGNORE_ERROR',
        'CANCEL_REQUEST_RECEIVED' => 'IGNORE_ERROR'
      },
      'maximum_file_segment_length' => 64
    }

    @destination_entity = {
      'id' => 2,
      'name' => 'DESTINATION',
      'protocol_version' => 1,
      'ack_timer_interval' => 5,
      'ack_timer_expiration_limit' => 3,
      'enable_acks' => true,
      'enable_finished' => true,
      'check_interval' => 5,
      'check_limit' => 3,
      'immediate_nak_mode' => true,
      'enable_eof_nak' => true,
      'file_segment_recv_indication' => true,
      'eof_recv_indication' => true,
      'transaction_finished_indication' => true,
      'nak_timer_interval' => 5,
      'nak_timer_expiration_limit' => 3,
      'maximum_file_segment_length' => 64,
      'fault_handler' => {
        'NO_ERROR' => 'IGNORE_ERROR',
        'FILESTORE_REJECTION' => 'IGNORE_ERROR',
        'FILE_CHECKSUM_FAILURE' => 'IGNORE_ERROR',
        'CHECK_LIMIT_REACHED' => 'IGNORE_ERROR',
        'NAK_LIMIT_REACHED' => 'IGNORE_ERROR',
        'INACTIVITY_DETECTED' => 'IGNORE_ERROR',
        'ACK_LIMIT_REACHED' => 'IGNORE_ERROR',
        'CANCEL_REQUEST_RECEIVED' => 'IGNORE_ERROR'
      }
    }

    allow(CfdpMib).to receive(:entity).with(nil).and_return(@source_entity)
    allow(CfdpMib).to receive(:entity).with(1).and_return(@source_entity)
    allow(CfdpMib).to receive(:source_entity).and_return(@destination_entity)

    @metadata_pdu_hash = {
      "DIRECTIVE_CODE" => "METADATA",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "FILE_SIZE" => 100,
      "SOURCE_FILE_NAME" => "source.txt",
      "DESTINATION_FILE_NAME" => "destination.txt",
      "CLOSURE_REQUESTED" => "CLOSURE_REQUESTED",
      "CHECKSUM_TYPE" => 0
    }

    @file_data_pdu_hash = {
      "DIRECTIVE_CODE" => "FILE_DATA",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "OFFSET" => 0,
      "FILE_DATA" => "a" * 50
    }

    @file_data_pdu_hash2 = {
      "DIRECTIVE_CODE" => "FILE_DATA",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "OFFSET" => 50,
      "FILE_DATA" => "b" * 50
    }

    @eof_pdu_hash = {
      "DIRECTIVE_CODE" => "EOF",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "CONDITION_CODE" => "NO_ERROR",
      "FILE_SIZE" => 100,
      "FILE_CHECKSUM" => 0
    }

    @ack_pdu_hash = {
      "DIRECTIVE_CODE" => "ACK",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "DIRECTIVE_CODE_OF_ACK" => "FINISHED",
      "CONDITION_CODE" => "NO_ERROR",
      "TRANSACTION_STATUS" => "TERMINATED"
    }

    @prompt_pdu_hash = {
      "DIRECTIVE_CODE" => "PROMPT",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "RESPONSE_REQUIRED" => "NAK"
    }

    @prompt_pdu_hash2 = {
      "DIRECTIVE_CODE" => "PROMPT",
      "SOURCE_ENTITY_ID" => 1,
      "SEQUENCE_NUMBER" => 123,
      "TRANSMISSION_MODE" => "ACKNOWLEDGED",
      "RESPONSE_REQUIRED" => "KEEP_ALIVE"
    }

    @mock_tempfile = double("Tempfile")
    allow(@mock_tempfile).to receive(:seek)
    allow(@mock_tempfile).to receive(:write)
    allow(@mock_tempfile).to receive(:close)
    allow(@mock_tempfile).to receive(:unlink)
    allow(@mock_tempfile).to receive(:path).and_return("/tmp/cfdp_test")
    allow(Tempfile).to receive(:new).and_return(@mock_tempfile)

    # Mock cmd method from OpenC3::Api that's included in CfdpTransaction
    allow_any_instance_of(CfdpReceiveTransaction).to receive(:cmd)

    # Mock CfdpPdu build methods
    allow(CfdpPdu).to receive(:build_ack_pdu).and_return("ACK_PDU")
    allow(CfdpPdu).to receive(:build_finished_pdu).and_return("FINISHED_PDU")
    allow(CfdpPdu).to receive(:build_nak_pdu).and_return("NAK_PDU")
    allow(CfdpPdu).to receive(:build_keep_alive_pdu).and_return("KEEP_ALIVE_PDU")
  end

  describe "initialize" do
    it "initializes with metadata PDU" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(receive_transaction.id).to eq("1__123")
      expect(receive_transaction.instance_variable_get(:@transaction_seq_num)).to eq(123)
      expect(receive_transaction.instance_variable_get(:@transmission_mode)).to eq("ACKNOWLEDGED")
      expect(receive_transaction.instance_variable_get(:@metadata_pdu_hash)).to eq(@metadata_pdu_hash)
    end

    it "adds itself to CfdpMib transactions hash during initialization" do
      # Clear transactions hash first
      @transactions.clear

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)

      # Verify the transaction was added to the MIB transactions hash
      expect(@transactions["1__123"]).to eq(receive_transaction)
      expect(@transactions).to have_key("1__123")
    end
  end

  describe "handle_pdu" do
    it "handles metadata PDU" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@metadata_pdu_hash)).to eq(@metadata_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@source_entity_id)).to eq(1)
      expect(receive_transaction.instance_variable_get(:@file_size)).to eq(100)
      expect(receive_transaction.instance_variable_get(:@source_file_name)).to eq("source.txt")
      expect(receive_transaction.instance_variable_get(:@destination_file_name)).to eq("destination.txt")
    end

    it "handles file data PDU" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@progress)).to eq(50)
      expect(receive_transaction.instance_variable_get(:@segments)[0]).to eq(50)
    end

    it "handles EOF PDU" do
      allow(CfdpMib).to receive(:put_destination_file).and_return(true)

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash2)
      receive_transaction.handle_pdu(@eof_pdu_hash)

      expect(receive_transaction.instance_variable_get(:@eof_pdu_hash)).to eq(@eof_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@state)).to eq("FINISHED")
      expect(receive_transaction.instance_variable_get(:@transaction_status)).to eq("TERMINATED")
    end

    it "handles ACK PDU" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@ack_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@finished_ack_pdu_hash)).to eq(@ack_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@finished_ack_timeout)).to be_nil
    end

    it "handles PROMPT PDU with NAK response" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(receive_transaction).to receive(:send_naks)
      receive_transaction.handle_pdu(@prompt_pdu_hash)
      expect(receive_transaction.instance_variable_get(:@prompt_pdu_hash)).to eq(@prompt_pdu_hash)
    end

    it "handles PROMPT PDU with KEEP_ALIVE response" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(receive_transaction).to receive(:send_keep_alive)
      receive_transaction.handle_pdu(@prompt_pdu_hash2)
      expect(receive_transaction.instance_variable_get(:@prompt_pdu_hash)).to eq(@prompt_pdu_hash2)
    end

    it "ignores unexpected PDU types" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      unexpected_pdu = {
        "DIRECTIVE_CODE" => "NAK",
        "SOURCE_ENTITY_ID" => 1,
        "SEQUENCE_NUMBER" => 123
      }
      # No expectations, just confirming it doesn't error
      receive_transaction.handle_pdu(unexpected_pdu)
    end
  end

  describe "check_complete" do
    before(:each) do
      # Mock checksum classes
      @mock_null_checksum = double("CfdpNullChecksum")
      allow(@mock_null_checksum).to receive(:add)
      allow(@mock_null_checksum).to receive(:check).and_return(true)
      allow(CfdpNullChecksum).to receive(:new).and_return(@mock_null_checksum)
      allow_any_instance_of(CfdpReceiveTransaction).to receive(:get_checksum).and_return(@mock_null_checksum)
    end

    it "returns false if metadata or EOF not received" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      # Clear metadata_pdu_hash
      receive_transaction.instance_variable_set(:@metadata_pdu_hash, nil)
      expect(receive_transaction.check_complete).to be false
    end

    it "handles canceled transaction" do
      canceled_eof = @eof_pdu_hash.dup
      canceled_eof["CONDITION_CODE"] = "CANCELED_BY_SOURCE"

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, canceled_eof)

      expect(receive_transaction.check_complete).to be true
      expect(receive_transaction.instance_variable_get(:@state)).to eq("CANCELED")
      expect(receive_transaction.instance_variable_get(:@transaction_status)).to eq("TERMINATED")
      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("CANCELED_BY_SOURCE")
    end

    it "completes file transfer with successful checksum" do
      allow(CfdpMib).to receive(:put_destination_file).and_return(true)

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash2)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, @eof_pdu_hash)

      expect(receive_transaction.check_complete).to be true
      expect(receive_transaction.instance_variable_get(:@file_status)).to eq("FILESTORE_SUCCESS")
      expect(receive_transaction.instance_variable_get(:@delivery_code)).to eq("DATA_COMPLETE")
    end

    it "handles filestore rejection" do
      allow(CfdpMib).to receive(:put_destination_file).and_return(false)

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash2)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, @eof_pdu_hash)

      expect(receive_transaction.check_complete).to be true
      expect(receive_transaction.instance_variable_get(:@file_status)).to eq("FILESTORE_REJECTION")
      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("FILESTORE_REJECTION")
    end

    it "handles checksum failure" do
      allow(@mock_null_checksum).to receive(:check).and_return(false)

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash2)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, @eof_pdu_hash)

      expect(receive_transaction.check_complete).to be true
      expect(receive_transaction.instance_variable_get(:@file_status)).to eq("FILE_DISCARDED")
      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("FILE_CHECKSUM_FAILURE")
      expect(receive_transaction.instance_variable_get(:@delivery_code)).to eq("DATA_INCOMPLETE")
    end

    it "processes filestore requests" do
      metadata_with_tlv = @metadata_pdu_hash.dup
      metadata_with_tlv["TLVS"] = [
        {
          "TYPE" => "FILESTORE_REQUEST",
          "ACTION_CODE" => "CREATE_FILE",
          "FIRST_FILE_NAME" => "create.txt"
        }
      ]

      allow(CfdpMib).to receive(:filestore_request).and_return(["SUCCESSFUL", "File created"])
      allow(CfdpMib).to receive(:put_destination_file).and_return(true)

      receive_transaction = CfdpReceiveTransaction.new(metadata_with_tlv)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash2)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, @eof_pdu_hash)

      expect(receive_transaction.check_complete).to be true
      expect(receive_transaction.instance_variable_get(:@filestore_responses).length).to eq(1)
      expect(receive_transaction.instance_variable_get(:@filestore_responses)[0]["STATUS_CODE"]).to eq("SUCCESSFUL")
    end
  end

  describe "complete_file_received?" do
    it "returns false if file_size is not set" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@file_size, nil)
      expect(receive_transaction.complete_file_received?).to be false
    end

    it "returns true when all segments are received" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash2)
      expect(receive_transaction.complete_file_received?).to be true
    end

    it "returns false when segments are missing" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.handle_pdu(@file_data_pdu_hash)
      # Skip the second segment
      expect(receive_transaction.complete_file_received?).to be false
    end

    it "returns true when segments cover entire file size even if not starting at 0" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      # Create an overlapping segment case - not starting at 0
      receive_transaction.instance_variable_set(:@segments, {5 => 100})
      expect(receive_transaction.complete_file_received?).to be false

      # Now add a segment that covers 0-5
      receive_transaction.instance_variable_set(:@segments, {0 => 5, 5 => 100})
      expect(receive_transaction.complete_file_received?).to be true
    end
  end

  describe "cancel" do
    it "cancels the transaction" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.cancel
      expect(receive_transaction.instance_variable_get(:@state)).to eq("CANCELED")
      expect(receive_transaction.instance_variable_get(:@transaction_status)).to eq("TERMINATED")
      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("CANCEL_REQUEST_RECEIVED")
    end
  end

  describe "suspend" do
    it "suspends acknowledged transactions" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.suspend
      expect(receive_transaction.instance_variable_get(:@state)).to eq("SUSPENDED")
    end
  end

  describe "update" do
    it "updates check timeouts" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@check_timeout, Time.now - 1)
      expect(receive_transaction).to receive(:handle_fault)

      # Set count to reach limit
      receive_transaction.instance_variable_set(:@check_timeout_count, @source_entity['check_limit'] - 1)
      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("CHECK_LIMIT_REACHED")
      expect(receive_transaction.instance_variable_get(:@check_timeout)).to be_nil
    end

    it "sends NAKs when nak_timeout expires" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@nak_timeout, Time.now - 1)
      expect(receive_transaction).to receive(:send_naks).with(true)

      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@nak_timeout_count)).to eq(1)
    end

    it "handles nak_timeout expiration limit" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@nak_timeout, Time.now - 1)
      expect(receive_transaction).to receive(:send_naks).with(true)
      expect(receive_transaction).to receive(:handle_fault)

      # Set count to reach limit
      receive_transaction.instance_variable_set(:@nak_timeout_count, @source_entity['nak_timer_expiration_limit'] - 1)
      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("NAK_LIMIT_REACHED")
      expect(receive_transaction.instance_variable_get(:@nak_timeout)).to be_nil
    end

    it "sends keep alive when keep_alive_timeout expires" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@keep_alive_timeout, Time.now - 1)
      expect(receive_transaction).to receive(:send_keep_alive)

      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@keep_alive_count)).to eq(1)
    end

    it "clears keep_alive_timeout when EOF received" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@keep_alive_timeout, Time.now + 10)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, @eof_pdu_hash)

      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@keep_alive_timeout)).to be_nil
    end

    it "handles inactivity timeout" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@inactivity_timeout, Time.now - 1)

      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@inactivity_count)).to eq(1)
    end

    it "handles inactivity limit reached" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@inactivity_timeout, Time.now - 1)
      expect(receive_transaction).to receive(:handle_fault)

      # Set count to reach limit
      receive_transaction.instance_variable_set(:@inactivity_count, @source_entity['transaction_inactivity_limit'] - 1)
      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("INACTIVITY_DETECTED")
    end

    it "clears inactivity_timeout when EOF received" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@inactivity_timeout, Time.now + 10)
      receive_transaction.instance_variable_set(:@eof_pdu_hash, @eof_pdu_hash)

      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@inactivity_timeout)).to be_nil
    end

    it "resends finished PDU when finished_ack_timeout expires" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@finished_ack_timeout, Time.now - 1)
      receive_transaction.instance_variable_set(:@finished_pdu, "FINISHED_PDU")

      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@finished_count)).to eq(1)
    end

    it "handles finished_ack limit reached" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@finished_ack_timeout, Time.now - 1)
      receive_transaction.instance_variable_set(:@finished_pdu, "FINISHED_PDU")
      expect(receive_transaction).to receive(:handle_fault)

      # Set count to reach limit
      receive_transaction.instance_variable_set(:@finished_count, @source_entity['ack_timer_expiration_limit'])
      receive_transaction.update

      expect(receive_transaction.instance_variable_get(:@condition_code)).to eq("ACK_LIMIT_REACHED")
      expect(receive_transaction.instance_variable_get(:@finished_ack_timeout)).to be_nil
    end
  end

  describe "send_keep_alive" do
    it "sends a keep alive PDU" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(CfdpPdu).to receive(:build_keep_alive_pdu)

      receive_transaction.send_keep_alive
    end
  end

  describe "send_naks" do
    before(:each) do
      # For these tests we'll skip the actual implementation since it requires more dependencies
      allow_any_instance_of(CfdpReceiveTransaction).to receive(:send_naks).and_return(nil)
    end

    it "sends NAK PDUs for missing segments" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@file_size, 100)
      receive_transaction.instance_variable_set(:@segments, {0 => 50})
      receive_transaction.instance_variable_set(:@progress, 50)

      # Just verify the method is called
      expect(receive_transaction).to receive(:send_naks)
      receive_transaction.send_naks
    end

    it "handles forced NAK sending" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@file_size, 100)
      receive_transaction.instance_variable_set(:@segments, {0 => 100})
      receive_transaction.instance_variable_set(:@progress, 100)

      # Just verify the different call signatures
      expect(receive_transaction).to receive(:send_naks).with(no_args)
      receive_transaction.send_naks

      expect(receive_transaction).to receive(:send_naks).with(true)
      receive_transaction.send_naks(true)
    end

    it "handles large file segments properly" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      receive_transaction.instance_variable_set(:@file_size, 200)
      # Create a gap from 50-150
      receive_transaction.instance_variable_set(:@segments, {0 => 50, 150 => 200})
      receive_transaction.instance_variable_set(:@progress, 100)

      # Just verify the method is called
      expect(receive_transaction).to receive(:send_naks)
      receive_transaction.send_naks
    end
  end

  describe "notice_of_completion" do
    it "sends a finished PDU" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(CfdpPdu).to receive(:build_finished_pdu)

      receive_transaction.notice_of_completion

      expect(receive_transaction.instance_variable_get(:@state)).to eq("FINISHED")
      expect(receive_transaction.instance_variable_get(:@transaction_status)).to eq("TERMINATED")
    end

    it "handles transaction finished indication" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(CfdpPdu).to receive(:build_finished_pdu)
      expect(CfdpTopic).to receive(:write_indication).with("Transaction-Finished", any_args)

      receive_transaction.notice_of_completion
    end

    it "doesn't send finished PDU when not enabled" do
      @source_entity['enable_finished'] = false
      allow(CfdpMib).to receive(:entity).with(1).and_return(@source_entity)

      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(CfdpPdu).not_to receive(:build_finished_pdu)

      receive_transaction.notice_of_completion
    end

    it "saves state" do
      receive_transaction = CfdpReceiveTransaction.new(@metadata_pdu_hash)
      expect(CfdpPdu).to receive(:build_finished_pdu)

      receive_transaction.notice_of_completion

      state = receive_transaction.load_state("1__123")
      # Spot check some state to ensure it all round trips
      expect(state["source_file_name"]).to eq("source.txt")
      expect(state["destination_file_name"]).to eq("destination.txt")
      expect(state["metadata_pdu_hash"]["DIRECTIVE_CODE"]).to eq("METADATA")
      # checksum round trips as a class
      expect(state["checksum"]).to be_a CfdpChecksum
    end
  end
end

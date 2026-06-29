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
require 'openc3/models/microservice_model'
require 'openc3/utilities/store_autoload'

RSpec.describe CfdpUser, type: :model do
  before(:each) do
    mock_redis()
    @source_entity_id = 1
    @destination_entity_id = 2
    ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
    ENV['OPENC3_SCOPE'] = 'DEFAULT'
    # Create the model that is consumed by CfdpMib.setup
    model = OpenC3::MicroserviceModel.new(name: ENV['OPENC3_MICROSERVICE_NAME'], scope: "DEFAULT",
      options: [
        ["source_entity_id", @source_entity_id],
        ["destination_entity_id", @destination_entity_id],
        ["root_path", SPEC_DIR],
      ],
    )
    model.create
    CfdpMib.clear
    CfdpMib.setup

    # Mock JsonPacket
    @packet = double("packet")
    allow(OpenC3::JsonPacket).to receive(:new).and_return(@packet)

    @user = CfdpUser.new
  end

  after(:each) do
    @user.stop
    # Clear MIB state between tests
    CfdpMib.clear
  end

  describe "initialize" do
    it "initializes instance variables" do
      expect(@user.instance_variable_get(:@thread)).to be_nil
      expect(@user.instance_variable_get(:@cancel_thread)).to be false
      expect(@user.instance_variable_get(:@item_name_lookup)).to eq({})
      expect(@user.instance_variable_get(:@source_transactions)).to eq([])
      expect(@user.instance_variable_get(:@source_threads)).to eq([])
    end
  end

  describe "receive_packet" do
    it "receives and decoms a packet" do
      # Setup mock objects
      topic = "DEFAULT__DECOM__{CFDP}__PDU"
      msg_id = "msg123"
      msg_hash = {"time" => "123456", "stored" => "TRUE", "json_data" => "{\"data\":\"test\"}"}
      redis = double("redis")

      # Setup expected behavior
      @user.instance_variable_set(:@item_name_lookup, {topic => "DATA"})
      pdu_data = "binary_pdu_data"
      pdu_hash = {"key" => "value"}

      allow(@packet).to receive(:read).with("DATA").and_return(pdu_data)
      allow(CfdpPdu).to receive(:decom).with(pdu_data).and_return(pdu_hash)

      # Test
      result = @user.receive_packet(topic, msg_id, msg_hash, redis)
      expect(result).to eq(pdu_hash)
    end
  end

  describe "start_source_transaction" do
    it "creates and starts a new source transaction" do
      # Setup mock thread
      thread = double("thread")
      allow(Thread).to receive(:new).and_yield.and_return(thread)
      allow(thread).to receive(:alive?).and_return(false)

      # Setup transaction
      transaction = double("transaction")
      allow(transaction).to receive(:id).and_return("transaction_1")
      allow(CfdpSourceTransaction).to receive(:new).and_return(transaction)
      allow(transaction).to receive(:proxy_response_info=)
      allow(transaction).to receive(:put)
      allow(transaction).to receive(:save_state)

      # Mock logger
      allow(OpenC3::Logger).to receive(:info)

      # Call the method
      params = {
        destination_entity_id: @destination_entity_id,
        source_file_name: "source.txt",
        destination_file_name: "dest.txt"
      }

      result = @user.start_source_transaction(params)

      # Verify results
      expect(result).to eq(transaction)
      expect(@user.instance_variable_get(:@source_transactions)).to include(transaction)
      expect(@user.instance_variable_get(:@source_threads)).to include(thread)
    end

    it "creates and starts a new proxy put transaction" do
      # Setup mock thread
      thread = double("thread")
      allow(Thread).to receive(:new).and_yield.and_return(thread)
      allow(thread).to receive(:alive?).and_return(false)

      # Setup transaction
      transaction = double("transaction")
      allow(transaction).to receive(:id).and_return("transaction_1")
      allow(CfdpSourceTransaction).to receive(:new).and_return(transaction)
      allow(transaction).to receive(:proxy_response_info=)
      allow(transaction).to receive(:put)
      allow(transaction).to receive(:save_state)

      # Mock logger
      allow(OpenC3::Logger).to receive(:info)

      # Call the method
      params = {
        remote_entity_id: 3,
        destination_entity_id: @destination_entity_id,
        source_file_name: "source.txt",
        destination_file_name: "dest.txt"
      }

      result = @user.start_source_transaction(params)

      # Verify results
      expect(result).to eq(transaction)
      expect(@user.instance_variable_get(:@source_transactions)).to include(transaction)
      expect(@user.instance_variable_get(:@source_threads)).to include(thread)
    end

    it "builds all optional proxy put messages" do
      # Setup mock thread that runs the block immediately
      thread = double("thread")
      allow(Thread).to receive(:new).and_yield.and_return(thread)
      allow(thread).to receive(:alive?).and_return(false)

      # Setup transaction
      transaction = double("transaction")
      allow(transaction).to receive(:id).and_return("transaction_1")
      allow(CfdpSourceTransaction).to receive(:new).and_return(transaction)
      allow(transaction).to receive(:proxy_response_info=)
      allow(transaction).to receive(:save_state)

      # Mock logger
      allow(OpenC3::Logger).to receive(:info)

      # Capture the messages_to_user that get built and passed to put
      captured = nil
      allow(transaction).to receive(:put) do |**kwargs|
        captured = kwargs
      end

      # Params exercising every optional proxy put message branch
      params = {
        remote_entity_id: 3,
        destination_entity_id: @destination_entity_id,
        source_file_name: "source.txt",
        destination_file_name: "dest.txt",
        messages_to_user: ["custom message"],
        filestore_requests: [["CREATE_FILE", "f.txt", "g.txt"]],
        fault_handler_overrides: [["NO_ERROR", "IGNORE_ERROR"]],
        transmission_mode: "UNACKNOWLEDGED",
        flow_label: "mylabel",
        segmentation_control: "NOT_PRESERVED",
        closure_requested: "CLOSURE_REQUESTED"
      }

      result = @user.start_source_transaction(params)

      # Verify the proxy put was performed with a populated messages_to_user list
      expect(result).to eq(transaction)
      expect(captured[:destination_entity_id]).to eq(3)
      expect(captured[:messages_to_user]).to be_a(Array)
      # Proxy put request + custom message + filestore + fault handler +
      # transmission mode + flow label + segmentation control + closure request
      expect(captured[:messages_to_user].length).to eq(8)
    end
  end

  describe "proxy_request_setup" do
    it "sets up a proxy request" do
      params = {
        remote_entity_id: @destination_entity_id
      }

      pdu, entity_id, messages_to_user = @user.proxy_request_setup(params)

      expect(pdu).to be_a(CfdpPdu)
      expect(entity_id).to eq(@destination_entity_id)
      expect(messages_to_user).to eq([])
    end
  end

  describe "proxy_request_start" do
    it "starts a proxy request" do
      # Setup
      entity_id = @destination_entity_id
      messages_to_user = ["message1", "message2"]

      # Mock start_source_transaction
      expect(@user).to receive(:start_source_transaction).with(
        {
          destination_entity_id: entity_id,
          messages_to_user: messages_to_user
        }
      ).and_return("transaction")

      # Call and verify
      result = @user.proxy_request_start(entity_id: entity_id, messages_to_user: messages_to_user)
      expect(result).to eq("transaction")
    end
  end

  describe "start_directory_listing" do
    it "starts a directory listing request" do
      # Setup
      pdu = instance_double(CfdpPdu)
      allow(pdu).to receive(:build_directory_listing_request_message).and_return("message")

      # Mock proxy_request_setup
      expect(@user).to receive(:proxy_request_setup).with(
        {
          remote_entity_id: @destination_entity_id,
          directory_name: "/tmp",
          directory_file_name: "listing.txt"
        }
      ).and_return([pdu, @destination_entity_id, []])

      # Mock proxy_request_start
      expect(@user).to receive(:proxy_request_start).with(
        entity_id: @destination_entity_id,
        messages_to_user: ["message"]
      ).and_return("transaction")

      # Call and verify
      params = {
        remote_entity_id: @destination_entity_id,
        directory_name: "/tmp",
        directory_file_name: "listing.txt"
      }
      result = @user.start_directory_listing(params)
      expect(result).to eq("transaction")
    end

    it "raises an error if directory_name is missing" do
      params = {
        remote_entity_id: @destination_entity_id,
        directory_file_name: "listing.txt"
      }

      expect { @user.start_directory_listing(params) }.to raise_error(/directory_name required/)
    end

    it "raises an error if directory_file_name is missing" do
      params = {
        remote_entity_id: @destination_entity_id,
        directory_name: "/tmp"
      }

      expect { @user.start_directory_listing(params) }.to raise_error(/directory_file_name required/)
    end
  end

  describe "cancel" do
    it "cancels a remote transaction" do
      # Setup
      pdu = instance_double(CfdpPdu)
      transaction_id = "1__123"
      allow(pdu).to receive(:build_proxy_put_cancel_message).and_return("cancel_message")
      allow(pdu).to receive(:build_originating_transaction_id_message).and_return("id_message")

      # Mock proxy_request_setup
      expect(@user).to receive(:proxy_request_setup).with(
        {
          remote_entity_id: @destination_entity_id,
          transaction_id: transaction_id
        }
      ).and_return([pdu, @destination_entity_id, []])

      # Mock proxy_request_start
      expect(@user).to receive(:proxy_request_start).with(
        entity_id: @destination_entity_id,
        messages_to_user: ["cancel_message", "id_message"]
      ).and_return("transaction")

      # Call and verify
      params = {
        remote_entity_id: @destination_entity_id,
        transaction_id: transaction_id
      }
      result = @user.cancel(params)
      expect(result).to eq("transaction")
    end

    it "cancels a local transaction" do
      # Setup a transaction in the MIB
      transaction = double("transaction")
      transaction_id = "1__123"
      allow(transaction).to receive(:cancel).and_return(true)
      CfdpMib.transactions[transaction_id] = transaction

      # Call and verify
      params = {
        transaction_id: transaction_id
      }
      result = @user.cancel(params)
      expect(result).to eq(transaction)
    end

    it "returns nil when transaction not found" do
      params = {
        transaction_id: "1__999" # Does not exist
      }
      result = @user.cancel(params)
      expect(result).to be_nil
    end
  end

  describe "suspend" do
    it "suspends a remote transaction" do
      # Setup
      pdu = instance_double(CfdpPdu)
      transaction_id = "1__123"
      allow(pdu).to receive(:build_remote_suspend_request_message).and_return("suspend_message")

      # Mock proxy_request_setup
      expect(@user).to receive(:proxy_request_setup).with(
        {
          remote_entity_id: @destination_entity_id,
          transaction_id: transaction_id
        }
      ).and_return([pdu, @destination_entity_id, []])

      # Mock proxy_request_start
      expect(@user).to receive(:proxy_request_start).with(
        entity_id: @destination_entity_id,
        messages_to_user: ["suspend_message"]
      ).and_return("transaction")

      # Call and verify
      params = {
        remote_entity_id: @destination_entity_id,
        transaction_id: transaction_id
      }
      result = @user.suspend(params)
      expect(result).to eq("transaction")
    end

    it "suspends a local transaction" do
      # Setup a transaction in the MIB
      transaction = double("transaction")
      transaction_id = "1__123"
      allow(transaction).to receive(:suspend).and_return(true)
      CfdpMib.transactions[transaction_id] = transaction

      # Call and verify
      params = {
        transaction_id: transaction_id
      }
      result = @user.suspend(params)
      expect(result).to eq(transaction)
    end
  end

  describe "resume" do
    it "resumes a remote transaction" do
      # Setup
      pdu = instance_double(CfdpPdu)
      transaction_id = "1__123"
      allow(pdu).to receive(:build_remote_resume_request_message).and_return("resume_message")

      # Mock proxy_request_setup
      expect(@user).to receive(:proxy_request_setup).with(
        {
          remote_entity_id: @destination_entity_id,
          transaction_id: transaction_id
        }
      ).and_return([pdu, @destination_entity_id, []])

      # Mock proxy_request_start
      expect(@user).to receive(:proxy_request_start).with(
        entity_id: @destination_entity_id,
        messages_to_user: ["resume_message"]
      ).and_return("transaction")

      # Call and verify
      params = {
        remote_entity_id: @destination_entity_id,
        transaction_id: transaction_id
      }
      result = @user.resume(params)
      expect(result).to eq("transaction")
    end

    it "resumes a local transaction" do
      # Setup a transaction in the MIB
      transaction = double("transaction")
      transaction_id = "1__123"
      allow(transaction).to receive(:resume).and_return(true)
      CfdpMib.transactions[transaction_id] = transaction

      # Call and verify
      params = {
        transaction_id: transaction_id
      }
      result = @user.resume(params)
      expect(result).to eq(transaction)
    end
  end

  describe "report" do
    it "requests a report from a remote transaction" do
      # Setup
      pdu = instance_double(CfdpPdu)
      transaction_id = "1__123"
      allow(pdu).to receive(:build_remote_status_report_request_message).and_return("report_message")

      # Mock proxy_request_setup
      expect(@user).to receive(:proxy_request_setup).with(
        {
          remote_entity_id: @destination_entity_id,
          transaction_id: transaction_id,
          report_file_name: "report.txt"
        }
      ).and_return([pdu, @destination_entity_id, []])

      # Mock proxy_request_start
      expect(@user).to receive(:proxy_request_start).with(
        entity_id: @destination_entity_id,
        messages_to_user: ["report_message"]
      ).and_return("transaction")

      # Call and verify
      params = {
        remote_entity_id: @destination_entity_id,
        transaction_id: transaction_id,
        report_file_name: "report.txt"
      }
      result = @user.report(params)
      expect(result).to eq("transaction")
    end

    it "raises an error if report_file_name is missing for remote reports" do
      params = {
        remote_entity_id: @destination_entity_id,
        transaction_id: "1__123"
      }

      expect { @user.report(params) }.to raise_error(/report_file_name required/)
    end

    it "generates a report for a local transaction" do
      # Setup a transaction in the MIB
      transaction = double("transaction")
      transaction_id = "1__123"
      allow(transaction).to receive(:report).and_return(true)
      CfdpMib.transactions[transaction_id] = transaction

      # Call and verify
      params = {
        transaction_id: transaction_id
      }
      result = @user.report(params)
      expect(result).to eq(transaction)
    end
  end

  describe "handle_messages_to_user" do
    it "handles proxy put request message" do
      # Mock start_source_transaction
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      # Call the method
      metadata_pdu_hash = {
        "SOURCE_ENTITY_ID" => @source_entity_id,
        "SEQUENCE_NUMBER" => 123
      }
      messages_to_user = [
        {
          "MSG_TYPE" => "PROXY_PUT_REQUEST",
          "DESTINATION_ENTITY_ID" => @destination_entity_id,
          "SOURCE_FILE_NAME" => "source.txt",
          "DESTINATION_FILE_NAME" => "dest.txt"
        }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles proxy put response message" do
      # Expect CfdpTopic.write_indication to be called
      expect(CfdpTopic).to receive(:write_indication).with('Proxy-Put-Response',
        transaction_id: "1__123",
        condition_code: "NO_ERROR",
        file_status: "FILESTORE_SUCCESS",
        delivery_code: "DATA_COMPLETE"
      )

      # Call the method
      metadata_pdu_hash = {
        "SOURCE_ENTITY_ID" => @source_entity_id,
        "SEQUENCE_NUMBER" => 456
      }
      messages_to_user = [
        {
          "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID",
          "SOURCE_ENTITY_ID" => 1,
          "SEQUENCE_NUMBER" => 123
        },
        {
          "MSG_TYPE" => "PROXY_PUT_RESPONSE",
          "CONDITION_CODE" => "NO_ERROR",
          "DELIVERY_CODE" => "DATA_COMPLETE",
          "FILE_STATUS" => "FILESTORE_SUCCESS"
        }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles directory listing request" do
      # Mock CfdpMib.directory_listing
      allow(CfdpMib).to receive(:directory_listing).and_return('{"result": "success"}')

      # Mock start_source_transaction
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      # Call the method
      metadata_pdu_hash = {
        "SOURCE_ENTITY_ID" => @source_entity_id,
        "SEQUENCE_NUMBER" => 123
      }
      messages_to_user = [
        {
          "MSG_TYPE" => "DIRECTORY_LISTING_REQUEST",
          "DIRECTORY_NAME" => "/tmp",
          "DIRECTORY_FILE_NAME" => "listing.txt"
        }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles unknown message types" do
      # Mock logger
      allow(OpenC3::Logger).to receive(:warn)

      # Mock is_printable
      allow(String).to receive(:instance_method).with(:is_printable?).and_return(proc { true })

      # Call the method
      metadata_pdu_hash = {
        "SOURCE_ENTITY_ID" => @source_entity_id,
        "SEQUENCE_NUMBER" => 123
      }
      messages_to_user = [
        {
          "MSG_TYPE" => "UNKNOWN",
          "DATA" => "some data",
          "MSG_TYPE_VALUE" => 99
        }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)

      # Verify logger was called
      expect(OpenC3::Logger).to have_received(:warn)
    end

    it "accumulates all optional proxy put parameters" do
      # Capture the params passed to start_source_transaction
      captured = nil
      expect(@user).to receive(:start_source_transaction) do |params, proxy_response_info: nil|
        captured = params
        "transaction"
      end

      metadata_pdu_hash = {
        "SOURCE_ENTITY_ID" => @source_entity_id,
        "SEQUENCE_NUMBER" => 123
      }
      messages_to_user = [
        {
          "MSG_TYPE" => "PROXY_PUT_REQUEST",
          "DESTINATION_ENTITY_ID" => @destination_entity_id,
          "SOURCE_FILE_NAME" => "source.txt",
          "DESTINATION_FILE_NAME" => "dest.txt"
        },
        { "MSG_TYPE" => "PROXY_MESSAGE_TO_USER", "MESSAGE_TO_USER" => "custom" },
        { "MSG_TYPE" => "PROXY_FILESTORE_REQUEST", "ACTION_CODE" => "CREATE_FILE",
          "FIRST_FILE_NAME" => "f.txt", "SECOND_FILE_NAME" => "g.txt" },
        { "MSG_TYPE" => "PROXY_FAULT_HANDLER_OVERRIDE", "CONDITION_CODE" => "NO_ERROR",
          "HANDLER_CODE" => "IGNORE_ERROR" },
        { "MSG_TYPE" => "PROXY_TRANSMISSION_MODE", "TRANSMISSION_MODE" => "UNACKNOWLEDGED" },
        { "MSG_TYPE" => "PROXY_FLOW_LABEL", "FLOW_LABEL" => "mylabel" },
        { "MSG_TYPE" => "PROXY_SEGMENTATION_CONTROL", "SEGMENTATION_CONTROL" => "NOT_PRESERVED" },
        { "MSG_TYPE" => "PROXY_CLOSURE_REQUEST", "CLOSURE_REQUESTED" => "CLOSURE_REQUESTED" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)

      expect(captured[:destination_entity_id]).to eq(@destination_entity_id)
      expect(captured[:source_file_name]).to eq("source.txt")
      expect(captured[:destination_file_name]).to eq("dest.txt")
      expect(captured[:messages_to_user]).to include("custom")
      expect(captured[:filestore_requests]).to eq([["CREATE_FILE", "f.txt", "g.txt"]])
      expect(captured[:fault_handler_overrides]).to eq([["NO_ERROR", "IGNORE_ERROR"]])
      expect(captured[:transmission_mode]).to eq("UNACKNOWLEDGED")
      expect(captured[:flow_label]).to eq("mylabel")
      expect(captured[:segmentation_control]).to eq("NOT_PRESERVED")
      expect(captured[:closure_requested]).to eq("CLOSURE_REQUESTED")
    end

    it "builds a filestore request without a second file name" do
      captured = nil
      expect(@user).to receive(:start_source_transaction) do |params, proxy_response_info: nil|
        captured = params
        "transaction"
      end

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @source_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        {
          "MSG_TYPE" => "PROXY_PUT_REQUEST",
          "DESTINATION_ENTITY_ID" => @destination_entity_id,
          "SOURCE_FILE_NAME" => "source.txt",
          "DESTINATION_FILE_NAME" => "dest.txt"
        },
        { "MSG_TYPE" => "PROXY_FILESTORE_REQUEST", "ACTION_CODE" => "DELETE_FILE",
          "FIRST_FILE_NAME" => "f.txt" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)

      expect(captured[:filestore_requests]).to eq([["DELETE_FILE", "f.txt"]])
    end

    it "handles a proxy put cancel" do
      # Setup a matching transaction in the MIB
      transaction = double("transaction")
      allow(transaction).to receive(:proxy_response_info).and_return(
        { "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 })
      expect(transaction).to receive(:cancel).with(@source_entity_id)
      CfdpMib.transactions["5__10"] = transaction

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @source_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID", "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 },
        { "MSG_TYPE" => "PROXY_PUT_CANCEL" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a proxy put response with filestore responses" do
      filestore_response = { "ACTION_CODE" => "CREATE_FILE", "STATUS_CODE" => "SUCCESSFUL",
                             "FIRST_FILE_NAME" => "f.txt" }
      expect(CfdpTopic).to receive(:write_indication).with('Proxy-Put-Response',
        transaction_id: "5__10", condition_code: "NO_ERROR",
        file_status: "FILESTORE_SUCCESS", delivery_code: "DATA_COMPLETE",
        filestore_responses: [filestore_response])

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @source_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID", "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 },
        filestore_response.merge("MSG_TYPE" => "PROXY_FILESTORE_RESPONSE"),
        { "MSG_TYPE" => "PROXY_PUT_RESPONSE", "CONDITION_CODE" => "NO_ERROR",
          "DELIVERY_CODE" => "DATA_COMPLETE", "FILE_STATUS" => "FILESTORE_SUCCESS" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles an unsuccessful directory listing" do
      allow(CfdpMib).to receive(:directory_listing).and_return(nil)
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "DIRECTORY_LISTING_REQUEST", "DIRECTORY_NAME" => "/tmp",
          "DIRECTORY_FILE_NAME" => "listing.txt" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a directory listing response" do
      expect(CfdpTopic).to receive(:write_indication).with('Directory-Listing-Response',
        transaction_id: "5__10", response_code: "SUCCESSFUL",
        directory_name: "/tmp", directory_file_name: "listing.txt")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID", "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 },
        { "MSG_TYPE" => "DIRECTORY_LISTING_RESPONSE", "DIRECTORY_NAME" => "/tmp",
          "DIRECTORY_FILE_NAME" => "listing.txt", "RESPONSE_CODE" => "SUCCESSFUL" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote status report request for an existing transaction" do
      transaction = double("transaction")
      allow(transaction).to receive(:build_report).and_return("report contents")
      allow(transaction).to receive(:transaction_status).and_return("ACTIVE")
      CfdpMib.transactions["7__20"] = transaction
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "REMOTE_STATUS_REPORT_REQUEST", "SOURCE_ENTITY_ID" => 7,
          "SEQUENCE_NUMBER" => 20, "REPORT_FILE_NAME" => "report.txt" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote status report request for a missing transaction" do
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "REMOTE_STATUS_REPORT_REQUEST", "SOURCE_ENTITY_ID" => 7,
          "SEQUENCE_NUMBER" => 999, "REPORT_FILE_NAME" => "report.txt" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote status report response" do
      expect(CfdpTopic).to receive(:write_indication).with('Remote-Report-Response',
        transaction_id: "5__10", source_entity_id: 7, sequence_number: 20,
        transaction_status: "ACTIVE", response_code: "SUCCESSFUL")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID", "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 },
        { "MSG_TYPE" => "REMOTE_STATUS_REPORT_RESPONSE", "SOURCE_ENTITY_ID" => 7,
          "SEQUENCE_NUMBER" => 20, "TRANSACTION_STATUS" => "ACTIVE", "RESPONSE_CODE" => "SUCCESSFUL" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote suspend request for an existing transaction" do
      transaction = double("transaction")
      expect(transaction).to receive(:suspend)
      allow(transaction).to receive(:transaction_status).and_return("ACTIVE")
      allow(transaction).to receive(:state).and_return("SUSPENDED")
      CfdpMib.transactions["7__20"] = transaction
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "REMOTE_SUSPEND_REQUEST", "SOURCE_ENTITY_ID" => 7, "SEQUENCE_NUMBER" => 20 }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote suspend request for a missing transaction" do
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "REMOTE_SUSPEND_REQUEST", "SOURCE_ENTITY_ID" => 7, "SEQUENCE_NUMBER" => 999 }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote resume request for an existing transaction" do
      transaction = double("transaction")
      expect(transaction).to receive(:resume)
      allow(transaction).to receive(:transaction_status).and_return("ACTIVE")
      allow(transaction).to receive(:state).and_return("ACTIVE")
      CfdpMib.transactions["7__20"] = transaction
      expect(@user).to receive(:start_source_transaction).and_return("transaction")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "REMOTE_RESUME_REQUEST", "SOURCE_ENTITY_ID" => 7, "SEQUENCE_NUMBER" => 20 }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote suspend response" do
      expect(CfdpTopic).to receive(:write_indication).with('Remote-Suspend-Response',
        transaction_id: "5__10", source_entity_id: 7, sequence_number: 20,
        transaction_status: "ACTIVE", suspension_indicator: "SUSPENDED")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID", "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 },
        { "MSG_TYPE" => "REMOTE_SUSPEND_RESPONSE", "SOURCE_ENTITY_ID" => 7, "SEQUENCE_NUMBER" => 20,
          "SUSPENSION_INDICATOR" => "SUSPENDED", "TRANSACTION_STATUS" => "ACTIVE" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end

    it "handles a remote resume response" do
      expect(CfdpTopic).to receive(:write_indication).with('Remote-Resume-Response',
        transaction_id: "5__10", source_entity_id: 7, sequence_number: 20,
        transaction_status: "ACTIVE", suspension_indicator: "NOT_SUSPENDED")

      metadata_pdu_hash = { "SOURCE_ENTITY_ID" => @destination_entity_id, "SEQUENCE_NUMBER" => 1 }
      messages_to_user = [
        { "MSG_TYPE" => "ORIGINATING_TRANSACTION_ID", "SOURCE_ENTITY_ID" => 5, "SEQUENCE_NUMBER" => 10 },
        { "MSG_TYPE" => "REMOTE_RESUME_RESPONSE", "SOURCE_ENTITY_ID" => 7, "SEQUENCE_NUMBER" => 20,
          "SUSPENSION_INDICATOR" => "NOT_SUSPENDED", "TRANSACTION_STATUS" => "ACTIVE" }
      ]

      @user.handle_messages_to_user(metadata_pdu_hash, messages_to_user)
    end
  end

  describe "stop" do
    it "stops the @user thread and kills source threads" do
      # Setup
      thread = double("thread")
      source_thread1 = double("source_thread1")
      source_thread2 = double("source_thread2")
      transaction = double("transaction")

      allow(thread).to receive(:join)
      allow(source_thread1).to receive(:alive?).and_return(true)
      allow(source_thread1).to receive(:kill)
      allow(source_thread2).to receive(:alive?).and_return(false)
      allow(transaction).to receive(:save_state)

      @user.instance_variable_set(:@thread, thread)
      @user.instance_variable_set(:@source_threads, [source_thread1, source_thread2])
      @user.instance_variable_set(:@source_transactions, [transaction])

      # Allow sleep
      allow(@user).to receive(:sleep)

      # Call and verify
      @user.stop

      expect(@user.instance_variable_get(:@cancel_thread)).to be true
      expect(transaction).to have_received(:save_state)
      expect(thread).to have_received(:join)
      expect(source_thread1).to have_received(:kill)
      expect(@user.instance_variable_get(:@thread)).to be_nil
    end
  end
end
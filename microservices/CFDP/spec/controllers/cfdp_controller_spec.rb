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

$orig_cfdp_user = $cfdp_user

RSpec.describe CfdpController, type: :controller do
  before(:each) do
    @mock_user = double("CfdpUser")
    @mock_transaction = double("CfdpTransaction")
    allow(@mock_transaction).to receive(:id).and_return("1__123")
    $cfdp_user = @mock_user

    # Setup authorization mock
    allow(controller).to receive(:check_authorization).and_return(true)
    allow(controller).to receive(:authorization).and_return(true)
  end

  after(:each) do
    $cfdp_user = $orig_cfdp_user
  end

  describe "PUT requests" do
    it "handles put request successfully" do
      params = {
        destination_entity_id: "2",
        destination_file_name: "dest.txt",
        source_file_name: "source.txt",
        controller: "cfdp",
        action: "put"
      }

      # Expect CfdpUser to be called with proper parameters
      expect(@mock_user).to receive(:start_source_transaction).with(
        ActionController::Parameters.new(params)
      ).and_return(@mock_transaction)

      post :put, params: params

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("1__123")
    end

    it "requires destination_entity_id" do
      post :put, params: {}

      expect(response).to have_http_status(:bad_request)
      expect(JSON.parse(response.body)["message"]).to include("destination_entity_id")
    end

    it "handles errors" do
      allow(@mock_user).to receive(:start_source_transaction).and_raise("Test error")

      post :put, params: { destination_entity_id: "2" }

      expect(response).to have_http_status(:internal_server_error)
      expect(JSON.parse(response.body)["message"]).to include("Test error")
    end
  end

  describe "CANCEL requests" do
    it "handles cancel request successfully" do
      params = { action: "cancel", controller: "cfdp", transaction_id: "1__123"  }

      # Expect CfdpUser to be called with proper parameters
      expect(@mock_user).to receive(:cancel).with(
        ActionController::Parameters.new(params)
      ).and_return(@mock_transaction)

      post :cancel, params: params

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("1__123")
    end

    it "requires transaction_id" do
      post :cancel, params: {}

      expect(response).to have_http_status(:bad_request)
    end

    it "handles transaction not found" do
      allow(@mock_user).to receive(:cancel).and_return(nil)

      post :cancel, params: { transaction_id: "1__123" }

      expect(response).to have_http_status(:not_found)
      expect(JSON.parse(response.body)["message"]).to include("not found")
    end

    it "handles errors" do
      allow(@mock_user).to receive(:cancel).and_raise("Test error")

      post :cancel, params: { transaction_id: "1__123" }

      expect(response).to have_http_status(:internal_server_error)
      expect(JSON.parse(response.body)["message"]).to include("Test error")
    end
  end

  describe "SUSPEND requests" do
    it "handles suspend request successfully" do
      params = { transaction_id: "1__123", controller: "cfdp", action: "suspend" }

      # Expect CfdpUser to be called with proper parameters
      expect(@mock_user).to receive(:suspend).with(
        ActionController::Parameters.new(params)
      ).and_return(@mock_transaction)

      post :suspend, params: params

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("1__123")
    end

    it "requires transaction_id" do
      post :suspend, params: {}

      expect(response).to have_http_status(:bad_request)
    end

    it "handles transaction not found" do
      allow(@mock_user).to receive(:suspend).and_return(nil)

      post :suspend, params: { transaction_id: "1__123" }

      expect(response).to have_http_status(:not_found)
      expect(JSON.parse(response.body)["message"]).to include("not found")
    end
  end

  describe "RESUME requests" do
    it "handles resume request successfully" do
      params = { action: "resume", controller: "cfdp", transaction_id: "1__123" }

      # Expect CfdpUser to be called with proper parameters
      expect(@mock_user).to receive(:resume).with(
        ActionController::Parameters.new(params)
      ).and_return(@mock_transaction)

      post :resume, params: params

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("1__123")
    end

    it "requires transaction_id" do
      post :resume, params: {}

      expect(response).to have_http_status(:bad_request)
    end

    it "handles transaction not found" do
      allow(@mock_user).to receive(:resume).and_return(nil)

      post :resume, params: { transaction_id: "1__123" }

      expect(response).to have_http_status(:not_found)
      expect(JSON.parse(response.body)["message"]).to include("not found")
    end
  end

  describe "REPORT requests" do
    it "handles report request successfully" do
      params = { transaction_id: "1__123", controller: "cfdp", action: "report" }

      # Expect CfdpUser to be called with proper parameters
      expect(@mock_user).to receive(:report).with(
        ActionController::Parameters.new(params)
      ).and_return(@mock_transaction)

      post :report, params: params

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("1__123")
    end

    it "requires transaction_id" do
      post :report, params: {}

      expect(response).to have_http_status(:bad_request)
    end

    it "handles transaction not found" do
      allow(@mock_user).to receive(:report).and_return(nil)

      post :report, params: { transaction_id: "1__123" }

      expect(response).to have_http_status(:not_found)
      expect(JSON.parse(response.body)["message"]).to include("not found")
    end
  end

  describe "DIRECTORY_LISTING requests" do
    it "handles directory listing request successfully" do
      params = {
        remote_entity_id: "2",
        directory_name: "test_dir",
        directory_file_name: "result.txt",
        controller: "cfdp",
        action: "directory_listing"
      }

      # Expect CfdpUser to be called with proper parameters
      expect(@mock_user).to receive(:start_directory_listing).with(
        ActionController::Parameters.new(params)
      ).and_return(@mock_transaction)

      post :directory_listing, params: params

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("1__123")
    end

    it "requires remote_entity_id, directory_name and directory_file_name" do
      post :directory_listing, params: {}

      expect(response).to have_http_status(:bad_request)
      expect(JSON.parse(response.body)["message"]).to include("missing")
    end
  end

  describe "SUBSCRIBE requests" do
    before(:each) do
      allow(CfdpTopic).to receive(:subscribe_indications).and_return("0-0")
    end

    it "handles subscribe request successfully" do
      post :subscribe

      expect(response).to have_http_status(:success)
      expect(response.body).to eq("0-0")
    end

    it "handles errors" do
      allow(CfdpTopic).to receive(:subscribe_indications).and_raise("Test error")

      post :subscribe

      expect(response).to have_http_status(:internal_server_error)
      expect(JSON.parse(response.body)["message"]).to include("Test error")
    end
  end

  describe "INDICATIONS requests" do
    before(:each) do
      @indications = {
        continuation: "1-1",
        indications: [
          {
            "time" => Time.now.to_nsec_from_epoch,
            "indication_type" => "Transaction-Finished",
            "transaction_id" => "1__123",
            "condition_code" => "NO_ERROR"
          }
        ]
      }
      allow(CfdpTopic).to receive(:read_indications).and_return(@indications)
    end

    it "gets indications for all transactions" do
      get :indications

      expect(response).to have_http_status(:success)
      parsed = JSON.parse(response.body)
      expect(parsed["continuation"]).to eq("1-1")
      expect(parsed["indications"]).to be_an(Array)
      expect(parsed["indications"].length).to eq(1)
    end

    it "gets indications for a specific transaction" do
      get :indications, params: { transaction_id: "1__123" }

      expect(response).to have_http_status(:success)
      expect(CfdpTopic).to have_received(:read_indications).with(
        hash_including(transaction_id: "1__123")
      )
    end

    it "handles errors" do
      allow(CfdpTopic).to receive(:read_indications).and_raise("Test error")

      get :indications

      expect(response).to have_http_status(:internal_server_error)
      expect(JSON.parse(response.body)["message"]).to include("Test error")
    end
  end

  describe "TRANSACTIONS requests" do
    before(:each) do
      @transactions = {
        "1__123" => @mock_transaction,
        "1__124" => @mock_transaction
      }

      allow(CfdpMib).to receive(:transactions).and_return(@transactions)
      allow(CfdpMib).to receive(:cleanup_old_transactions)

      allow(@mock_transaction).to receive(:as_json).and_return({"id" => "1__123", "state" => "ACTIVE"})
      allow(@mock_transaction).to receive(:transaction_status).and_return("ACTIVE")
      allow(@mock_transaction).to receive(:id).and_return("1__123")
    end

    it "gets all transactions" do
      get :transactions

      expect(response).to have_http_status(:success)

      parsed = JSON.parse(response.body)
      expect(parsed).to be_an(Array)
      expect(parsed.length).to eq(2)
    end

    it "gets only active transactions" do
      get :transactions, params: { active: "true" }

      expect(response).to have_http_status(:success)

      parsed = JSON.parse(response.body)
      expect(parsed).to be_an(Array)
      expect(parsed.length).to eq(2)
    end

    it "handles errors" do
      allow(CfdpMib).to receive(:transactions).and_raise("Test error")

      get :transactions

      expect(response).to have_http_status(:internal_server_error)
      expect(JSON.parse(response.body)["message"]).to include("Test error")
    end
  end

  describe "check_authorization" do
    before(:each) do
      # Reset the mock to allow the real method to be called
      allow(controller).to receive(:check_authorization).and_call_original

      # Setup mocks needed by the method
      @entity = {
        "id" => 1,
        "cmd_info" => ["TARGET", "PACKET", "ITEM"],
        "tlm_info" => []
      }

      allow(CfdpMib).to receive(:entity).and_return(@entity)
      allow(CfdpMib).to receive(:source_entity_id).and_return(1)
      allow(CfdpMib).to receive(:cleanup_old_transactions)
    end

    it "authorizes when entity_id is numeric" do
      allow(controller).to receive(:authorization).and_return(true)

      expect(controller.send(:check_authorization)).to be true
    end
  end
end
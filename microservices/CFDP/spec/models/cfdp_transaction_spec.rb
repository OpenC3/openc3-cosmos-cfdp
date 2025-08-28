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

RSpec.describe CfdpTransaction do
  before(:each) do
    # Mock CfdpTopic
    allow(CfdpTopic).to receive(:write_indication)

    # Mock CfdpMib
    @source_entity = {
      'id' => 1,
      'name' => 'SOURCE',
      'fault_handler' => {
        'NO_ERROR' => 'IGNORE_ERROR',
        'FILESTORE_REJECTION' => 'ISSUE_NOTICE_OF_CANCELLATION',
        'FILE_CHECKSUM_FAILURE' => 'ISSUE_NOTICE_OF_SUSPENSION',
        'FILE_SIZE_ERROR' => 'ABANDON_TRANSACTION',
        'CHECK_LIMIT_REACHED' => 'IGNORE_ERROR'
      },
      'suspended_indication' => true,
      'resume_indication' => true,
      'keep_alive_interval' => 5
    }

    allow(CfdpMib).to receive(:source_entity).and_return(@source_entity)

    # Mock logging
    allow(OpenC3::Logger).to receive(:info)
    allow(OpenC3::Logger).to receive(:error)

    # Mock Api module
    allow_any_instance_of(CfdpTransaction).to receive(:cmd)

    @redis_prefix = 'CFDP_MICROSERVICE_NAME'
    allow(CfdpTransaction).to receive(:redis_key_prefix).and_return(@redis_prefix)

    ENV['OPENC3_SCOPE'] = 'DEFAULT'
  end

  describe "initialize" do
    it "initializes with default values" do
      transaction = CfdpTransaction.new

      expect(transaction.frozen).to be false
      expect(transaction.state).to eq("ACTIVE")
      expect(transaction.transaction_status).to eq("ACTIVE")
      expect(transaction.progress).to eq(0)
      expect(transaction.condition_code).to eq("NO_ERROR")
      expect(transaction.delivery_code).to be_nil
      expect(transaction.instance_variable_get(:@metadata_pdu_hash)).to be_nil
      expect(transaction.instance_variable_get(:@metadata_pdu_count)).to eq(0)
      expect(transaction.proxy_response_info).to be_nil
      expect(transaction.proxy_response_needed).to be false
      expect(transaction.instance_variable_get(:@source_file_name)).to be_nil
      expect(transaction.instance_variable_get(:@destination_file_name)).to be_nil
      expect(transaction.create_time).to be_a(Time)
      expect(transaction.complete_time).to be_nil
    end
  end

  describe "class methods" do
    it "builds a transaction id" do
      id = CfdpTransaction.build_transaction_id(1, 123)
      expect(id).to eq("1__123")
    end
  end

  describe "instance methods" do
    let(:transaction) { CfdpTransaction.new }

    describe "suspend" do
      it "suspends an active transaction" do
        transaction.suspend

        expect(transaction.state).to eq("SUSPENDED")
        expect(transaction.condition_code).to eq("SUSPEND_REQUEST_RECEIVED")
        expect(CfdpTopic).to have_received(:write_indication).with("Suspended", hash_including(transaction_id: nil, condition_code: "SUSPEND_REQUEST_RECEIVED"))
      end

      it "does nothing if transaction is not active" do
        transaction.instance_variable_set(:@state, "FINISHED")
        transaction.suspend

        expect(transaction.state).to eq("FINISHED")
      end
    end

    describe "resume" do
      it "resumes a suspended transaction" do
        transaction.instance_variable_set(:@state, "SUSPENDED")
        transaction.resume

        expect(transaction.state).to eq("ACTIVE")
        expect(transaction.condition_code).to eq("NO_ERROR")
        expect(CfdpTopic).to have_received(:write_indication).with("Resumed", hash_including(transaction_id: nil, progress: 0))
      end

      it "does nothing if transaction is not suspended" do
        transaction.resume

        expect(transaction.state).to eq("ACTIVE")
        expect(CfdpTopic).not_to have_received(:write_indication).with("Resumed", any_args)
      end
    end

    describe "cancel" do
      it "cancels an active transaction" do
        transaction.cancel

        expect(transaction.state).to eq("CANCELED")
        expect(transaction.transaction_status).to eq("TERMINATED")
        expect(transaction.condition_code).to eq("CANCEL_REQUEST_RECEIVED")
        expect(transaction.complete_time).to be_a(Time)
      end

      it "cancels with a canceling entity id" do
        transaction.cancel(3)

        expect(transaction.state).to eq("CANCELED")
        expect(transaction.instance_variable_get(:@canceling_entity_id)).to eq(3)
      end

      it "does nothing if transaction is already finished" do
        transaction.instance_variable_set(:@state, "FINISHED")
        original_time = Time.now.utc - 10
        transaction.instance_variable_set(:@complete_time, original_time)

        transaction.cancel

        expect(transaction.state).to eq("FINISHED")
        expect(transaction.complete_time).to eq(original_time)
      end
    end

    describe "abandon" do
      it "abandons an active transaction" do
        transaction.abandon

        expect(transaction.state).to eq("ABANDONED")
        expect(transaction.transaction_status).to eq("TERMINATED")
        expect(CfdpTopic).to have_received(:write_indication).with("Abandoned", hash_including(transaction_id: nil, condition_code: "NO_ERROR", progress: 0))
        expect(transaction.complete_time).to be_a(Time)
      end

      it "does nothing if transaction is already finished" do
        transaction.instance_variable_set(:@state, "FINISHED")
        original_time = Time.now.utc - 10
        transaction.instance_variable_set(:@complete_time, original_time)

        transaction.abandon

        expect(transaction.state).to eq("FINISHED")
        expect(transaction.complete_time).to eq(original_time)
      end
    end

    describe "report" do
      it "sends a report indication" do
        transaction.report

        expect(CfdpTopic).to have_received(:write_indication).with("Report", hash_including(transaction_id: nil))
      end
    end

    describe "freeze and unfreeze" do
      it "freezes the transaction" do
        transaction.freeze
        expect(transaction.instance_variable_get(:@frozen)).to be true
      end

      it "unfreezes the transaction" do
        transaction.instance_variable_set(:@frozen, true)
        transaction.unfreeze
        expect(transaction.instance_variable_get(:@frozen)).to be false
      end
    end

    describe "build_report" do
      it "generates a JSON report" do
        transaction.instance_variable_set(:@id, "1__123")
        report = transaction.build_report

        expect(report).to be_a(String)

        # Parse and verify JSON structure
        json = JSON.parse(report)
        expect(json["id"]).to eq("1__123")
        expect(json["state"]).to eq("ACTIVE")
        expect(json["transaction_status"]).to eq("ACTIVE")
        expect(json["progress"]).to eq(0)
        expect(json["frozen"]).to be false
      end
    end

    describe "as_json" do
      it "returns a hash representation of the transaction" do
        transaction.instance_variable_set(:@id, "1__123")
        transaction.instance_variable_set(:@source_file_name, "source.txt")
        transaction.instance_variable_set(:@destination_file_name, "dest.txt")

        json = transaction.as_json

        expect(json).to be_a(Hash)
        expect(json["id"]).to eq("1__123")
        expect(json["state"]).to eq("ACTIVE")
        expect(json["source_file_name"]).to eq("source.txt")
        expect(json["destination_file_name"]).to eq("dest.txt")
        expect(json["create_time"]).to be_a(String)
        expect(json["complete_time"]).to be_nil
      end

      it "includes complete_time when available" do
        transaction.instance_variable_set(:@complete_time, Time.now.utc)

        json = transaction.as_json

        expect(json["complete_time"]).to be_a(String)
      end
    end

    describe "handle_fault" do
      it "handles ISSUE_NOTICE_OF_CANCELLATION response" do
        transaction.instance_variable_set(:@condition_code, "FILESTORE_REJECTION")

        expect(transaction).to receive(:cancel)
        transaction.handle_fault
      end

      it "handles ISSUE_NOTICE_OF_SUSPENSION response" do
        transaction.instance_variable_set(:@condition_code, "FILE_CHECKSUM_FAILURE")

        expect(transaction).to receive(:suspend)
        transaction.handle_fault
      end

      it "handles ABANDON_TRANSACTION response" do
        transaction.instance_variable_set(:@condition_code, "FILE_SIZE_ERROR")

        expect(transaction).to receive(:abandon)
        transaction.handle_fault
      end

      it "handles IGNORE_ERROR response" do
        transaction.instance_variable_set(:@condition_code, "CHECK_LIMIT_REACHED")

        expect(transaction).to receive(:ignore_fault)
        transaction.handle_fault
      end

      it "uses fault handler overrides" do
        transaction.instance_variable_set(:@condition_code, "CHECK_LIMIT_REACHED")
        transaction.instance_variable_set(:@fault_handler_overrides, {"CHECK_LIMIT_REACHED" => "ISSUE_NOTICE_OF_CANCELLATION"})

        expect(transaction).to receive(:cancel)
        transaction.handle_fault
      end
    end

    describe "ignore_fault" do
      it "sends a fault indication" do
        transaction.instance_variable_set(:@condition_code, "FILE_SIZE_ERROR")

        transaction.ignore_fault

        expect(CfdpTopic).to have_received(:write_indication).with("Fault", hash_including(transaction_id: nil, condition_code: "FILE_SIZE_ERROR", progress: 0))
      end
    end

    describe "get_checksum" do
      it "returns a CfdpChecksum for type 0" do
        checksum = transaction.get_checksum(0)
        expect(checksum).to be_a(CfdpChecksum)
      end

      it "returns a CfdpCrcChecksum for type 1" do
        checksum = transaction.get_checksum(1)
        expect(checksum).to be_a(CfdpCrcChecksum)
      end

      it "returns a CfdpCrcChecksum for type 2" do
        checksum = transaction.get_checksum(2)
        expect(checksum).to be_a(CfdpCrcChecksum)
      end

      it "returns a CfdpCrcChecksum for type 3" do
        checksum = transaction.get_checksum(3)
        expect(checksum).to be_a(CfdpCrcChecksum)
      end

      it "returns a CfdpNullChecksum for type 15" do
        checksum = transaction.get_checksum(15)
        expect(checksum).to be_a(CfdpNullChecksum)
      end

      it "returns nil for unknown checksum types" do
        checksum = transaction.get_checksum(10)
        expect(checksum).to be_nil
      end
    end

    describe "cfdp_cmd" do
      it "sends a command with the correct parameters" do
        entity = {'cmd_delay' => 0.1}

        expect(transaction).to receive(:cmd).with('TARGET', 'PACKET', {'PARAM' => 'value'}, scope: 'DEFAULT')

        transaction.cfdp_cmd(entity, 'TARGET', 'PACKET', {'PARAM' => 'value'}, scope: 'DEFAULT')
      end
    end

    describe "save_state" do
      it "saves transaction state to OpenC3::Store" do
        mock_redis
        transaction.instance_variable_set(:@id, "1__123")
        transaction.instance_variable_set(:@source_file_name, "source.txt")
        transaction.instance_variable_set(:@destination_file_name, "dest.txt")
        transaction.instance_variable_set(:@transaction_seq_num, 123)

        allow(OpenC3::Logger).to receive(:debug)

        transaction.save_state

        expect(OpenC3::Store.hget("#{@redis_prefix}cfdp_transaction_state:1__123", "id")).to eq("1__123")
        expect(OpenC3::Store.hget("#{@redis_prefix}cfdp_transaction_state:1__123", "state")).to eq("ACTIVE")
        expect(OpenC3::Store.hget("#{@redis_prefix}cfdp_transaction_state:1__123", "source_file_name")).to eq("source.txt")
        expect(OpenC3::Store.hget("#{@redis_prefix}cfdp_transaction_state:1__123", "destination_file_name")).to eq("dest.txt")
        expect(OpenC3::Store.hget("#{@redis_prefix}cfdp_transaction_state:1__123", "transaction_seq_num")).to eq("123")
        expect(OpenC3::Logger).to have_received(:debug).with("CFDP Transaction 1__123 state saved", scope: 'DEFAULT')
      end

      it "handles nil values by not setting them in store" do
        mock_redis
        transaction.instance_variable_set(:@id, "1__123")

        allow(OpenC3::Logger).to receive(:debug)

        transaction.save_state

        expect(OpenC3::Store.hexists("#{@redis_prefix}cfdp_transaction_state:1__123", "delivery_code")).to be false
        expect(OpenC3::Store.hexists("#{@redis_prefix}cfdp_transaction_state:1__123", "file_status")).to be false
        expect(OpenC3::Store.hexists("#{@redis_prefix}cfdp_transaction_state:1__123", "complete_time")).to be false
      end
    end

    describe "load_state" do
      it "loads transaction state from OpenC3::Store" do
        mock_redis

        state_key = "#{@redis_prefix}cfdp_transaction_state:1__123"
        OpenC3::Store.hset(state_key, "id", "1__123")
        OpenC3::Store.hset(state_key, "frozen", "false")
        OpenC3::Store.hset(state_key, "state", "SUSPENDED")
        OpenC3::Store.hset(state_key, "transaction_status", "ACTIVE")
        OpenC3::Store.hset(state_key, "progress", "50")
        OpenC3::Store.hset(state_key, "transaction_seq_num", "123")
        OpenC3::Store.hset(state_key, "condition_code", "SUSPEND_REQUEST_RECEIVED")
        OpenC3::Store.hset(state_key, "source_file_name", "source.txt")
        OpenC3::Store.hset(state_key, "destination_file_name", "dest.txt")
        OpenC3::Store.hset(state_key, "create_time", "2023-01-01T10:00:00.000000Z")
        OpenC3::Store.hset(state_key, "proxy_response_needed", "true")
        OpenC3::Store.hset(state_key, "metadata_pdu_count", "5")
        OpenC3::Store.hset(state_key, "fault_handler_overrides", '{"FILE_SIZE_ERROR": "ABANDON_TRANSACTION"}')

        allow(OpenC3::Logger).to receive(:debug)

        result = transaction.load_state("1__123")

        expect(result).to be true
        expect(transaction.id).to eq("1__123")
        expect(transaction.frozen).to be false
        expect(transaction.state).to eq("SUSPENDED")
        expect(transaction.transaction_status).to eq("ACTIVE")
        expect(transaction.progress).to eq(50)
        expect(transaction.transaction_seq_num).to eq(123)
        expect(transaction.condition_code).to eq("SUSPEND_REQUEST_RECEIVED")
        expect(transaction.instance_variable_get(:@source_file_name)).to eq("source.txt")
        expect(transaction.instance_variable_get(:@destination_file_name)).to eq("dest.txt")
        expect(transaction.create_time).to be_a(Time)
        expect(transaction.proxy_response_needed).to be true
        expect(transaction.metadata_pdu_count).to eq(5)
        expect(transaction.instance_variable_get(:@fault_handler_overrides)).to eq({"FILE_SIZE_ERROR" => "ABANDON_TRANSACTION"})
        expect(OpenC3::Logger).to have_received(:debug).with("CFDP Transaction 1__123 state loaded", scope: 'DEFAULT')
      end

      it "returns false when no state data exists" do
        mock_redis
        allow(OpenC3::Logger).to receive(:debug)

        result = transaction.load_state("nonexistent__123")

        expect(result).to be false
      end

      it "handles missing fields with defaults" do
        mock_redis

        OpenC3::Store.hset("#{@redis_prefix}cfdp_transaction_state:1__123", "id", "1__123")

        allow(OpenC3::Logger).to receive(:debug)

        result = transaction.load_state("1__123")

        expect(result).to be true
        expect(transaction.state).to eq("ACTIVE")
        expect(transaction.transaction_status).to eq("ACTIVE")
        expect(transaction.progress).to eq(0)
        expect(transaction.condition_code).to eq("NO_ERROR")
        expect(transaction.metadata_pdu_count).to eq(0)
        expect(transaction.instance_variable_get(:@fault_handler_overrides)).to eq({})
      end
    end

    describe "save and load state cycle" do
      it "preserves all transaction data through save/load cycle" do
        mock_redis

        original_time = Time.now.utc
        transaction.instance_variable_set(:@id, "1__456")
        transaction.instance_variable_set(:@frozen, true)
        transaction.instance_variable_set(:@state, "SUSPENDED")
        transaction.instance_variable_set(:@transaction_status, "ACTIVE")
        transaction.instance_variable_set(:@progress, 75)
        transaction.instance_variable_set(:@transaction_seq_num, 456)
        transaction.instance_variable_set(:@condition_code, "SUSPEND_REQUEST_RECEIVED")
        transaction.instance_variable_set(:@delivery_code, "DATA_COMPLETE")
        transaction.instance_variable_set(:@file_status, "FILESTORE_SUCCESS")
        transaction.instance_variable_set(:@metadata_pdu_hash, "abc123")
        transaction.instance_variable_set(:@metadata_pdu_count, 3)
        transaction.instance_variable_set(:@create_time, original_time)
        transaction.instance_variable_set(:@proxy_response_info, "proxy_info")
        transaction.instance_variable_set(:@proxy_response_needed, true)
        transaction.instance_variable_set(:@canceling_entity_id, 2)
        transaction.instance_variable_set(:@fault_handler_overrides, {"FILE_SIZE_ERROR" => "ABANDON_TRANSACTION"})
        transaction.instance_variable_set(:@source_file_name, "original.txt")
        transaction.instance_variable_set(:@destination_file_name, "copy.txt")

        allow(OpenC3::Logger).to receive(:debug)

        transaction.save_state

        new_transaction = CfdpTransaction.new
        result = new_transaction.load_state("1__456")

        expect(result).to be true
        expect(new_transaction.id).to eq("1__456")
        expect(new_transaction.frozen).to be true
        expect(new_transaction.state).to eq("SUSPENDED")
        expect(new_transaction.transaction_status).to eq("ACTIVE")
        expect(new_transaction.progress).to eq(75)
        expect(new_transaction.transaction_seq_num).to eq(456)
        expect(new_transaction.condition_code).to eq("SUSPEND_REQUEST_RECEIVED")
        expect(new_transaction.delivery_code).to eq("DATA_COMPLETE")
        expect(new_transaction.file_status).to eq("FILESTORE_SUCCESS")
        expect(new_transaction.metadata_pdu_hash).to eq("abc123")
        expect(new_transaction.metadata_pdu_count).to eq(3)
        expect(new_transaction.create_time.to_i).to eq(original_time.to_i)
        expect(new_transaction.complete_time).to be_nil # complete_time is not persisted
        expect(new_transaction.proxy_response_info).to eq("proxy_info")
        expect(new_transaction.proxy_response_needed).to be true
        expect(new_transaction.instance_variable_get(:@canceling_entity_id)).to eq(2)
        expect(new_transaction.instance_variable_get(:@fault_handler_overrides)).to eq({"FILE_SIZE_ERROR" => "ABANDON_TRANSACTION"})
        expect(new_transaction.instance_variable_get(:@source_file_name)).to eq("original.txt")
        expect(new_transaction.instance_variable_get(:@destination_file_name)).to eq("copy.txt")
      end
    end

    describe "saved transaction ID tracking" do
      it "tracks transaction IDs in Redis set when saving state" do
        mock_redis
        transaction.instance_variable_set(:@id, "1__789")

        allow(OpenC3::Logger).to receive(:debug)

        # Initially no saved transaction IDs
        expect(CfdpTransaction.get_saved_transaction_ids).to be_empty
        expect(CfdpTransaction.has_saved_state?("1__789")).to be false

        # Save state should add to set
        transaction.save_state

        saved_ids = CfdpTransaction.get_saved_transaction_ids
        expect(saved_ids).to include("1__789")
        expect(saved_ids.length).to eq(1)
        expect(CfdpTransaction.has_saved_state?("1__789")).to be true
      end

      it "does not duplicate transaction IDs in the set" do
        mock_redis
        transaction.instance_variable_set(:@id, "1__789")

        allow(OpenC3::Logger).to receive(:debug)

        # Save state multiple times
        transaction.save_state
        transaction.save_state
        transaction.save_state

        saved_ids = CfdpTransaction.get_saved_transaction_ids
        expect(saved_ids.count("1__789")).to eq(1)
        expect(saved_ids.length).to eq(1)
      end

      it "tracks multiple different transaction IDs" do
        mock_redis

        transaction1 = CfdpTransaction.new
        transaction1.instance_variable_set(:@id, "1__100")
        transaction2 = CfdpTransaction.new
        transaction2.instance_variable_set(:@id, "2__200")

        allow(OpenC3::Logger).to receive(:debug)

        transaction1.save_state
        transaction2.save_state

        saved_ids = CfdpTransaction.get_saved_transaction_ids
        expect(saved_ids).to include("1__100")
        expect(saved_ids).to include("2__200")
        expect(saved_ids.length).to eq(2)
        expect(CfdpTransaction.has_saved_state?("1__100")).to be true
        expect(CfdpTransaction.has_saved_state?("2__200")).to be true
      end

      it "removes transaction ID and state when requested" do
        mock_redis
        transaction.instance_variable_set(:@id, "1__999")

        allow(OpenC3::Logger).to receive(:debug)

        # Save state
        transaction.save_state
        expect(CfdpTransaction.get_saved_transaction_ids).to include("1__999")
        expect(CfdpTransaction.has_saved_state?("1__999")).to be true
        expect(OpenC3::Store.exists("#{@redis_prefix}cfdp_transaction_state:1__999")).to be > 0

        # Remove saved state
        transaction.remove_saved_state

        expect(CfdpTransaction.get_saved_transaction_ids).not_to include("1__999")
        expect(CfdpTransaction.has_saved_state?("1__999")).to be false
        expect(OpenC3::Store.exists("#{@redis_prefix}cfdp_transaction_state:1__999")).to eq(0)
      end

      it "clears all saved transaction IDs" do
        mock_redis

        transaction1 = CfdpTransaction.new
        transaction1.instance_variable_set(:@id, "1__111")
        transaction2 = CfdpTransaction.new
        transaction2.instance_variable_set(:@id, "2__222")

        allow(OpenC3::Logger).to receive(:debug)

        transaction1.save_state
        transaction2.save_state

        expect(CfdpTransaction.get_saved_transaction_ids.length).to eq(2)
        expect(CfdpTransaction.has_saved_state?("1__111")).to be true
        expect(CfdpTransaction.has_saved_state?("2__222")).to be true

        CfdpTransaction.clear_saved_transaction_ids

        expect(CfdpTransaction.get_saved_transaction_ids).to be_empty
        expect(CfdpTransaction.has_saved_state?("1__111")).to be false
        expect(CfdpTransaction.has_saved_state?("2__222")).to be false
      end

      it "automatically removes saved state when transaction completes" do
        mock_redis
        transaction.instance_variable_set(:@id, "1__completed")

        allow(OpenC3::Logger).to receive(:info)
        allow(OpenC3::Logger).to receive(:debug)

        transaction.save_state
        expect(CfdpTransaction.has_saved_state?("1__completed")).to be true

        transaction.cancel
        expect(CfdpTransaction.has_saved_state?("1__completed")).to be false
        expect(CfdpTransaction.get_saved_transaction_ids).not_to include("1__completed")
      end
    end

    describe "update" do
      it "does nothing in the base class" do
        # Just verifying it doesn't raise an error
        transaction.update
      end
    end
  end
end

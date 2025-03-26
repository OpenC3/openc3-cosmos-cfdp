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
require 'tempfile'

RSpec.describe CfdpMib do
  before(:each) do
    allow(OpenC3::Logger).to receive(:info)
    allow(OpenC3::Logger).to receive(:error)
    CfdpMib.clear
  end

  describe "entity management" do
    it "defines an entity with default values" do
      entity = CfdpMib.define_entity(1)
      expect(entity['id']).to eq(1)
      expect(entity['protocol_version_number']).to eq(1)
      expect(entity['default_transmission_mode']).to eq('UNACKNOWLEDGED')
      expect(entity['fault_handler']["FILE_CHECKSUM_FAILURE"]).to eq("IGNORE_ERROR")
    end

    it "returns entity by id" do
      CfdpMib.define_entity(1)
      entity = CfdpMib.entity(1)
      expect(entity['id']).to eq(1)
    end

    it "sets source entity id" do
      CfdpMib.define_entity(1)
      CfdpMib.source_entity_id = 1
      expect(CfdpMib.source_entity_id).to eq(1)
    end

    it "returns source entity" do
      CfdpMib.define_entity(1)
      CfdpMib.source_entity_id = 1
      entity = CfdpMib.source_entity
      expect(entity['id']).to eq(1)
    end
  end

  describe "set_entity_value" do
    before(:each) do
      CfdpMib.define_entity(1)
    end

    it "sets integer values" do
      CfdpMib.set_entity_value(1, 'protocol_version_number', 2)
      expect(CfdpMib.entity(1)['protocol_version_number']).to eq(2)
    end

    it "sets boolean values" do
      CfdpMib.set_entity_value(1, 'immediate_nak_mode', false)
      expect(CfdpMib.entity(1)['immediate_nak_mode']).to eq(false)
    end

    it "sets transmission mode values" do
      CfdpMib.set_entity_value(1, 'default_transmission_mode', 'ACKNOWLEDGED')
      expect(CfdpMib.entity(1)['default_transmission_mode']).to eq('ACKNOWLEDGED')
    end

    it "sets cmd_info values" do
      CfdpMib.set_entity_value(1, 'cmd_info', ['TARGET', 'PACKET', 'ITEM'])
      expect(CfdpMib.entity(1)['cmd_info']).to eq(['TARGET', 'PACKET', 'ITEM'])
    end

    it "adds tlm_info values" do
      CfdpMib.set_entity_value(1, 'tlm_info', ['TARGET', 'PACKET', 'ITEM'])
      expect(CfdpMib.entity(1)['tlm_info']).to eq([['TARGET', 'PACKET', 'ITEM']])
    end

    it "raises an error for unknown options" do
      expect { CfdpMib.set_entity_value(1, 'unknown_option', 'value') }.to raise_error(RuntimeError, /Unknown OPTION/)
    end

    it "raises an error for invalid tlm_info" do
      expect { CfdpMib.set_entity_value(1, 'tlm_info', ['TARGET', 'PACKET']) }.to raise_error(RuntimeError, /Invalid tlm_info/)
    end

    it "raises an error for invalid cmd_info" do
      expect { CfdpMib.set_entity_value(1, 'cmd_info', ['TARGET', 'PACKET']) }.to raise_error(RuntimeError, /Invalid cmd_info/)
    end
  end

  describe "file operations" do
    before(:each) do
      CfdpMib.root_path = "/tmp"
      @tmp_file = Tempfile.new('cfdp_test')
      @tmp_file.write("test data")
      @tmp_file.close
    end

    after(:each) do
      @tmp_file.unlink if @tmp_file
    end

    it "gets a source file" do
      allow(File).to receive(:open).and_return(@tmp_file)
      file = CfdpMib.get_source_file("test.txt")
      expect(file).to eq(@tmp_file)
    end

    it "handles missing source files" do
      allow(File).to receive(:open).and_raise(Errno::ENOENT.new("No such file"))
      file = CfdpMib.get_source_file("missing.txt")
      expect(file).to be_nil
    end

    it "completes a source file" do
      allow(@tmp_file).to receive(:close)
      CfdpMib.complete_source_file(@tmp_file)
      # Just verifying no errors are raised
    end

    it "puts a destination file" do
      # Create a fake tempfile for testing
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_return(true)
      allow(temp).to receive(:unlink).and_return(true)
      allow(temp).to receive(:open).and_return(temp)
      allow(temp).to receive(:read).and_return("test data")

      result = CfdpMib.put_destination_file("test_dest.txt", temp)
      expect(result).to be true
    end

    it "handles errors while putting destination files" do
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_raise("File write error")

      result = CfdpMib.put_destination_file("test_dest.txt", temp)
      expect(result).to be false
    end
  end

  describe "filestore_request" do
    before(:each) do
      CfdpMib.root_path = "/tmp"
      @tmp_file1 = Tempfile.new('cfdp_test1')
      @tmp_file1.write("test data 1")
      @tmp_file1.close

      @tmp_file2 = Tempfile.new('cfdp_test2')
      @tmp_file2.write("test data 2")
      @tmp_file2.close

      # Setup mocks for File operations
      allow(File).to receive(:absolute_path) { |path| path }
      allow(File).to receive(:exist?).and_return(true)
      allow(FileUtils).to receive(:touch).and_return(true)
      allow(FileUtils).to receive(:rm).and_return(true)
      allow(FileUtils).to receive(:mv).and_return(true)
      allow(FileUtils).to receive(:mkdir).and_return(true)
      allow(FileUtils).to receive(:rmdir).and_return(true)
      allow(File).to receive(:open).and_yield(StringIO.new)
      allow(File).to receive(:read).and_return("test data")
      allow(Dir).to receive(:exist?).and_return(true)
    end

    after(:each) do
      @tmp_file1.unlink if @tmp_file1
      @tmp_file2.unlink if @tmp_file2
    end

    it "handles CREATE_FILE action" do
      status, message = CfdpMib.filestore_request("CREATE_FILE", "test_create.txt", nil)
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles DELETE_FILE action when file exists" do
      status, message = CfdpMib.filestore_request("DELETE_FILE", "test_delete.txt", nil)
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles DELETE_FILE action when file doesn't exist" do
      allow(File).to receive(:exist?).and_return(false)
      status, message = CfdpMib.filestore_request("DELETE_FILE", "missing.txt", nil)
      expect(status).to eq("FILE_DOES_NOT_EXIST")
    end

    it "handles RENAME_FILE action" do
      allow(File).to receive(:exist?).with("/tmp/old.txt").and_return(true)
      allow(File).to receive(:exist?).with("/tmp/new.txt").and_return(false)
      status, message = CfdpMib.filestore_request("RENAME_FILE", "old.txt", "new.txt")
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles RENAME_FILE when destination already exists" do
      # First call to exist? for the destination file
      allow(File).to receive(:exist?).with("/tmp/new.txt").and_return(true)

      status, message = CfdpMib.filestore_request("RENAME_FILE", "old.txt", "new.txt")
      expect(status).to eq("NEW_FILE_ALREADY_EXISTS")
    end

    it "handles APPEND_FILE action" do
      status, message = CfdpMib.filestore_request("APPEND_FILE", "file1.txt", "file2.txt")
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles REPLACE_FILE action" do
      status, message = CfdpMib.filestore_request("REPLACE_FILE", "file1.txt", "file2.txt")
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles CREATE_DIRECTORY action" do
      status, message = CfdpMib.filestore_request("CREATE_DIRECTORY", "new_dir", nil)
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles REMOVE_DIRECTORY action" do
      status, message = CfdpMib.filestore_request("REMOVE_DIRECTORY", "test_dir", nil)
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles DENY_FILE action" do
      status, message = CfdpMib.filestore_request("DENY_FILE", "test_file.txt", nil)
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles DENY_DIRECTORY action" do
      status, message = CfdpMib.filestore_request("DENY_DIRECTORY", "test_dir", nil)
      expect(status).to eq("SUCCESSFUL")
    end

    it "handles unknown action codes" do
      status, message = CfdpMib.filestore_request("UNKNOWN_ACTION", "file.txt", nil)
      expect(status).to eq("NOT_PERFORMED")
      expect(message).to include("Unknown action code")
    end

    it "handles file path safety" do
      allow(File).to receive(:absolute_path).with("/tmp/../dangerous.txt").and_return("/dangerous.txt")

      status, message = CfdpMib.filestore_request("CREATE_FILE", "../dangerous.txt", nil)
      expect(status).to eq("NOT_ALLOWED")
      expect(message).to include("Dangerous filename")
    end

    it "handles exceptions during file operations" do
      allow(FileUtils).to receive(:touch).and_raise("File system error")

      status, message = CfdpMib.filestore_request("CREATE_FILE", "test.txt", nil)
      expect(status).to eq("NOT_ALLOWED")
      expect(message).to include("File system error")
    end
  end

  describe "directory_listing" do
    before(:each) do
      CfdpMib.root_path = "/tmp"

      # Setup mocks
      allow(File).to receive(:absolute_path) { |path| path }
      allow(File).to receive(:join) { |*args| args.join('/') }
      allow(Dir).to receive(:entries).and_return(['.', '..', 'file1.txt', 'file2.txt', 'subdir'])
      allow(File).to receive(:directory?).with("/tmp/test_dir/subdir").and_return(true)
      allow(File).to receive(:directory?).with("/tmp/test_dir/file1.txt").and_return(false)
      allow(File).to receive(:directory?).with("/tmp/test_dir/file2.txt").and_return(false)

      file_stat = double("File::Stat")
      allow(file_stat).to receive(:mtime).and_return(Time.now)
      allow(file_stat).to receive(:size).and_return(1024)
      allow(File).to receive(:stat).and_return(file_stat)
    end

    it "returns a JSON listing of files and directories" do
      result = CfdpMib.directory_listing("test_dir", "result.txt")
      expect(result).to be_a(String)

      # Parse the JSON and verify it contains the expected entries
      json = JSON.parse(result)
      expect(json).to be_an(Array)
      expect(json.size).to eq(3) # file1.txt, file2.txt, subdir

      # Check for directory and file entries
      dir_entry = json.find { |entry| entry["directory"] == "subdir" }
      expect(dir_entry).to be_present

      file_entry = json.find { |entry| entry["name"] == "file1.txt" }
      expect(file_entry).to be_present
      expect(file_entry["size"]).to eq(1024)
    end

    it "handles file path safety" do
      allow(File).to receive(:absolute_path).with("/tmp/../dangerous").and_return("/dangerous")

      result = CfdpMib.directory_listing("../dangerous", "result.txt")
      expect(result).to be_nil
    end
  end

  describe "setup" do
    before(:each) do
      @mock_model = double("MicroserviceModel")
      @options = [
        ["source_entity_id", "1"],
        ["destination_entity_id", "2"],
        ["root_path", "/tmp"],
        ["protocol_version_number", "1"],
        ["ack_timer_interval", "300"],
        ["immediate_nak_mode", "true"],
        ["cmd_info", "TARGET", "PACKET", "ITEM"],
        ["tlm_info", "TARGET", "TLM_PKT", "ITEM"]
      ]
      allow(@mock_model).to receive(:options).and_return(@options)
      allow(OpenC3::MicroserviceModel).to receive(:get_model).and_return(@mock_model)
    end

    it "initializes MIB from options" do
      CfdpMib.setup

      # Verify entities were created
      expect(CfdpMib.source_entity_id).to eq(1)
      expect(CfdpMib.entity(1)).to be_present
      expect(CfdpMib.entity(2)).to be_present

      # Verify options were applied
      expect(CfdpMib.entity(2)['protocol_version_number']).to eq(1)
      expect(CfdpMib.entity(2)['ack_timer_interval']).to eq(300)
      expect(CfdpMib.entity(2)['immediate_nak_mode']).to eq(true)
      expect(CfdpMib.entity(2)['cmd_info']).to eq(["TARGET", "PACKET", "ITEM"])
      expect(CfdpMib.entity(2)['tlm_info']).to include(["TARGET", "TLM_PKT", "ITEM"])

      # Verify root path was set
      expect(CfdpMib.root_path).to eq("/tmp")
    end

    it "raises error when required options are missing" do
      # Remove required options
      @options.delete_if { |opt| opt[0] == "source_entity_id" }

      expect { CfdpMib.setup }.to raise_error(RuntimeError, /OPTION source_entity_id is required/)
    end
  end

  describe "cleanup_old_transactions" do
    before(:each) do
      # Setup entities
      CfdpMib.define_entity(1)
      CfdpMib.source_entity_id = 1
      CfdpMib.entity(1)['transaction_retain_seconds'] = 60

      # Create mock transactions
      @active_tx = double("Transaction")
      allow(@active_tx).to receive(:complete_time).and_return(nil)

      @recent_tx = double("Transaction")
      allow(@recent_tx).to receive(:complete_time).and_return(Time.now.utc - 30)

      @old_tx = double("Transaction")
      allow(@old_tx).to receive(:complete_time).and_return(Time.now.utc - 120)

      # Add transactions to the MIB
      CfdpMib.transactions["tx1"] = @active_tx
      CfdpMib.transactions["tx2"] = @recent_tx
      CfdpMib.transactions["tx3"] = @old_tx
    end

    it "removes old completed transactions" do
      # Verify there are 3 transactions before cleanup
      expect(CfdpMib.transactions.size).to eq(3)

      # Run cleanup
      CfdpMib.cleanup_old_transactions

      # Verify only old transaction was removed
      expect(CfdpMib.transactions.size).to eq(2)
      expect(CfdpMib.transactions).to have_key("tx1")
      expect(CfdpMib.transactions).to have_key("tx2")
      expect(CfdpMib.transactions).not_to have_key("tx3")
    end
  end
end
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

RSpec.describe CfdpMib do
  before(:each) do
    allow(OpenC3::Logger).to receive(:info)
    allow(OpenC3::Logger).to receive(:error)
    allow(OpenC3::Logger).to receive(:debug)
    allow(CfdpTransaction).to receive(:clear_saved_transaction_ids)
    CfdpMib.clear

    @redis_prefix = 'CFDP_MICROSERVICE_NAME'
    allow(CfdpTransaction).to receive(:redis_key_prefix).and_return(@redis_prefix)
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

    it "puts a destination file when it doesn't exist" do
      # Create a fake tempfile for testing
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_return(true)
      allow(temp).to receive(:unlink).and_return(true)
      allow(temp).to receive(:open).and_return(temp)
      allow(temp).to receive(:read).and_return("test data")
      allow(File).to receive(:exist?).with("/tmp/test_dest.txt").and_return(false)

      result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
      expect(result).to be true
      expect(actual_filename).to eq("test_dest.txt")
    end

    it "puts a destination file with timestamp when it already exists" do
      # Create a fake tempfile for testing
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_return(true)
      allow(temp).to receive(:unlink).and_return(true)
      allow(temp).to receive(:open).and_return(temp)
      allow(temp).to receive(:read).and_return("test data")
      allow(File).to receive(:exist?).with("/tmp/test_dest.txt").and_return(true)

      # Mock time to get predictable timestamp
      frozen_time = Time.parse("2025-01-19 14:30:52 UTC")
      allow(Time).to receive(:now).and_return(frozen_time)

      result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
      expect(result).to be true
      expect(actual_filename).to eq("test_dest_20250119_143052.txt")
    end

    it "uses custom timestamp format when provided" do
      # Create a fake tempfile for testing
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_return(true)
      allow(temp).to receive(:unlink).and_return(true)
      allow(temp).to receive(:open).and_return(temp)
      allow(temp).to receive(:read).and_return("test data")
      allow(File).to receive(:exist?).with("/tmp/test_dest.txt").and_return(true)

      # Mock time to get predictable timestamp
      frozen_time = Time.parse("2025-01-19 14:30:52 UTC")
      allow(Time).to receive(:now).and_return(frozen_time)

      result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp, "-%Y%m%d%H%M%S")
      expect(result).to be true
      expect(actual_filename).to eq("test_dest-20250119143052.txt")
    end

    it "handles files without extensions when adding timestamps" do
      # Create a fake tempfile for testing
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_return(true)
      allow(temp).to receive(:unlink).and_return(true)
      allow(temp).to receive(:open).and_return(temp)
      allow(temp).to receive(:read).and_return("test data")
      allow(File).to receive(:exist?).with("/tmp/noext_file").and_return(true)

      # Mock time to get predictable timestamp
      frozen_time = Time.parse("2025-01-19 14:30:52 UTC")
      allow(Time).to receive(:now).and_return(frozen_time)

      result, actual_filename = CfdpMib.put_destination_file("noext_file", temp)
      expect(result).to be true
      expect(actual_filename).to eq("noext_file_20250119_143052")
    end

    it "handles errors while putting destination files" do
      temp = Tempfile.new('cfdp_dest')
      allow(temp).to receive(:persist).and_raise("File write error")

      result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
      expect(result).to be false
      expect(actual_filename).to be_nil
    end

    context "with bucket storage" do
      before(:each) do
        CfdpMib.bucket = "test-bucket"
        CfdpMib.prevent_received_file_overwrite = true
        @mock_client = double("Bucket Client")
        allow(OpenC3::Bucket).to receive(:getClient).and_return(@mock_client)
      end

      after(:each) do
        CfdpMib.bucket = nil
      end

      it "puts a destination file to bucket when it doesn't exist" do
        temp = Tempfile.new('cfdp_dest')
        allow(temp).to receive(:unlink).and_return(true)
        allow(temp).to receive(:open).and_return(temp)
        allow(temp).to receive(:read).and_return("test data")
        allow(@mock_client).to receive(:check_object).with(bucket: "test-bucket", key: "/tmp/test_dest.txt").and_return(false)
        allow(@mock_client).to receive(:put_object).and_return(true)

        result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
        expect(result).to be true
        expect(actual_filename).to eq("test_dest.txt")
      end

      it "puts a destination file to bucket with timestamp when it already exists" do
        temp = Tempfile.new('cfdp_dest')
        allow(temp).to receive(:unlink).and_return(true)
        allow(temp).to receive(:open).and_return(temp)
        allow(temp).to receive(:read).and_return("test data")
        allow(@mock_client).to receive(:check_object).with(bucket: "test-bucket", key: "/tmp/test_dest.txt").and_return(true)
        allow(@mock_client).to receive(:put_object).and_return(true)

        # Mock time to get predictable timestamp
        frozen_time = Time.parse("2025-01-19 14:30:52 UTC")
        allow(Time).to receive(:now).and_return(frozen_time)

        result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
        expect(result).to be true
        expect(actual_filename).to eq("test_dest_20250119_143052.txt")

        # Verify put_object was called with timestamped filename
        expect(@mock_client).to have_received(:put_object).with(
          bucket: "test-bucket",
          key: "/tmp/test_dest_20250119_143052.txt",
          body: "test data"
        )
      end

      it "handles bucket errors while putting destination files" do
        temp = Tempfile.new('cfdp_dest')
        allow(temp).to receive(:open).and_return(temp)
        allow(temp).to receive(:read).and_return("test data")
        allow(@mock_client).to receive(:check_object).and_raise("Bucket connection error")

        result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
        expect(result).to be false
        expect(actual_filename).to be_nil
      end
    end

    context "when prevent_received_file_overwrite is enabled" do
      before(:each) do
        CfdpMib.prevent_received_file_overwrite = true
      end

      it "saves new files with timestamps when there is a name conflict" do
        temp = Tempfile.new('cfdp_dest')
        allow(temp).to receive(:persist).and_return(true)
        allow(temp).to receive(:unlink).and_return(true)
        allow(temp).to receive(:open).and_return(temp)
        allow(temp).to receive(:read).and_return("new file data")
        allow(File).to receive(:exist?).with("/tmp/conflicting_file.txt").and_return(true)

        frozen_time = Time.parse("2025-01-19 15:45:30 UTC")
        allow(Time).to receive(:now).and_return(frozen_time)

        result, actual_filename = CfdpMib.put_destination_file("conflicting_file.txt", temp)
        expect(result).to be true
        expect(actual_filename).to eq("conflicting_file_20250119_154530.txt")
      end

      context "with bucket storage" do
        before(:each) do
          CfdpMib.bucket = "test-bucket"
          @mock_client = double("Bucket Client")
          allow(OpenC3::Bucket).to receive(:getClient).and_return(@mock_client)
        end

        after(:each) do
          CfdpMib.bucket = nil
        end

        it "saves new files with timestamps in bucket when there is a name conflict" do
          temp = Tempfile.new('cfdp_dest')
          allow(temp).to receive(:unlink).and_return(true)
          allow(temp).to receive(:open).and_return(temp)
          allow(temp).to receive(:read).and_return("new file data")
          allow(@mock_client).to receive(:check_object).with(bucket: "test-bucket", key: "/tmp/conflicting_file.txt").and_return(true)
          allow(@mock_client).to receive(:put_object).and_return(true)

          frozen_time = Time.parse("2025-01-19 15:45:30 UTC")
          allow(Time).to receive(:now).and_return(frozen_time)

          result, actual_filename = CfdpMib.put_destination_file("conflicting_file.txt", temp)
          expect(result).to be true
          expect(actual_filename).to eq("conflicting_file_20250119_154530.txt")

          expect(@mock_client).to have_received(:put_object).with(
            bucket: "test-bucket",
            key: "/tmp/conflicting_file_20250119_154530.txt",
            body: "new file data"
          )
        end
      end
    end

    context "when prevent_received_file_overwrite is disabled" do
      before(:each) do
        CfdpMib.prevent_received_file_overwrite = false
      end

      it "overwrites existing files without adding timestamp" do
        temp = Tempfile.new('cfdp_dest')
        allow(temp).to receive(:persist).and_return(true)
        allow(temp).to receive(:unlink).and_return(true)
        allow(temp).to receive(:open).and_return(temp)
        allow(temp).to receive(:read).and_return("test data")
        allow(File).to receive(:exist?).with("/tmp/test_dest.txt").and_return(true)

        result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
        expect(result).to be true
        expect(actual_filename).to eq("test_dest.txt")
      end

      it "handles files that don't exist normally" do
        temp = Tempfile.new('cfdp_dest')
        allow(temp).to receive(:persist).and_return(true)
        allow(temp).to receive(:unlink).and_return(true)
        allow(temp).to receive(:open).and_return(temp)
        allow(temp).to receive(:read).and_return("test data")
        allow(File).to receive(:exist?).with("/tmp/new_file.txt").and_return(false)

        result, actual_filename = CfdpMib.put_destination_file("new_file.txt", temp)
        expect(result).to be true
        expect(actual_filename).to eq("new_file.txt")
      end

      context "with bucket storage" do
        before(:each) do
          CfdpMib.bucket = "test-bucket"
          @mock_client = double("Bucket Client")
          allow(OpenC3::Bucket).to receive(:getClient).and_return(@mock_client)
        end

        after(:each) do
          CfdpMib.bucket = nil
        end

        it "overwrites existing files in bucket without adding timestamp" do
          temp = Tempfile.new('cfdp_dest')
          allow(temp).to receive(:unlink).and_return(true)
          allow(temp).to receive(:open).and_return(temp)
          allow(temp).to receive(:read).and_return("test data")
          allow(@mock_client).to receive(:check_object).with(bucket: "test-bucket", key: "/tmp/test_dest.txt").and_return(true)
          allow(@mock_client).to receive(:put_object).and_return(true)

          result, actual_filename = CfdpMib.put_destination_file("test_dest.txt", temp)
          expect(result).to be true
          expect(actual_filename).to eq("test_dest.txt")

          expect(@mock_client).to have_received(:put_object).with(
            bucket: "test-bucket",
            key: "/tmp/test_dest.txt",
            body: "test data"
          )
        end

        it "handles new files in bucket normally" do
          temp = Tempfile.new('cfdp_dest')
          allow(temp).to receive(:unlink).and_return(true)
          allow(temp).to receive(:open).and_return(temp)
          allow(temp).to receive(:read).and_return("test data")
          allow(@mock_client).to receive(:check_object).with(bucket: "test-bucket", key: "/tmp/new_file.txt").and_return(false)
          allow(@mock_client).to receive(:put_object).and_return(true)

          result, actual_filename = CfdpMib.put_destination_file("new_file.txt", temp)
          expect(result).to be true
          expect(actual_filename).to eq("new_file.txt")

          expect(@mock_client).to have_received(:put_object).with(
            bucket: "test-bucket",
            key: "/tmp/new_file.txt",
            body: "test data"
          )
        end
      end
    end
  end

  describe "list_directory_files" do
    before(:each) do
      CfdpMib.root_path = "/tmp"
    end

    context "when using filesystem" do
      before(:each) do
        CfdpMib.bucket = nil
        allow(Dir).to receive(:exist?).and_return(true)
        allow(Dir).to receive(:entries).and_return(['.', '..', 'file1.txt', 'file2.txt', 'subdir'])
        allow(File).to receive(:file?).with("/tmp/test_dir/file1.txt").and_return(true)
        allow(File).to receive(:file?).with("/tmp/test_dir/file2.txt").and_return(true)
        allow(File).to receive(:file?).with("/tmp/test_dir/subdir").and_return(false)
      end

      it "yields filenames for files in directory" do
        filenames = []
        CfdpMib.list_directory_files("test_dir") { |filename| filenames << filename }

        expect(filenames).to contain_exactly("test_dir/file1.txt", "test_dir/file2.txt")
      end

      it "handles nil directory name" do
        expect { |b| CfdpMib.list_directory_files(nil, &b) }.not_to yield_control
      end

      it "handles non-existent directory" do
        allow(Dir).to receive(:exist?).and_return(false)
        expect { |b| CfdpMib.list_directory_files("missing_dir", &b) }.not_to yield_control
      end

      it "logs errors when exceptions occur" do
        allow(Dir).to receive(:entries).and_raise(StandardError.new("Directory read error"))
        expect(OpenC3::Logger).to receive(:error).with("Directory read error", scope: ENV['OPENC3_SCOPE'])

        CfdpMib.list_directory_files("test_dir") { |filename| }
      end
    end

    context "when using S3 bucket" do
      before(:each) do
        @mock_client = double("S3Client")
        CfdpMib.bucket = "test-bucket"
        allow(OpenC3::Bucket).to receive(:getClient).and_return(@mock_client)

        @mock_objects = [
          { key: "/tmp/test_dir/file1.txt" },
          { key: "/tmp/test_dir/file2.txt" },
          { key: "/tmp/test_dir/subdir/" },  # Directory (ends with /)
        ]
        allow(@mock_client).to receive(:list_objects).and_return(@mock_objects)
      end

      it "yields filenames for objects in S3 directory" do
        filenames = []
        CfdpMib.list_directory_files("test_dir") { |filename| filenames << filename }

        expect(filenames).to contain_exactly("test_dir/file1.txt", "test_dir/file2.txt")
      end

      it "strips root path from S3 object keys" do
        @mock_objects = [
          { key: "/tmp/test_dir/file1.txt" },
          { key: "/tmp/another_dir/file2.txt" },
        ]
        allow(@mock_client).to receive(:list_objects).and_return(@mock_objects)

        filenames = []
        CfdpMib.list_directory_files("test_dir") { |filename| filenames << filename }

        expect(filenames).to contain_exactly("test_dir/file1.txt", "another_dir/file2.txt")
      end

      it "skips directory objects (ending with /)" do
        filenames = []
        CfdpMib.list_directory_files("test_dir") { |filename| filenames << filename }

        expect(filenames).not_to include("test_dir/subdir/")
      end

      it "handles S3 errors gracefully" do
        allow(@mock_client).to receive(:list_objects).and_raise(StandardError.new("S3 connection error"))
        expect(OpenC3::Logger).to receive(:error).with("S3 connection error", scope: ENV['OPENC3_SCOPE'])

        CfdpMib.list_directory_files("test_dir") { |filename| }
      end
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
      mock_redis

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
      mock_redis

      # Remove required options
      @options.delete_if { |opt| opt[0] == "source_entity_id" }

      expect { CfdpMib.setup }.to raise_error(RuntimeError, /OPTION source_entity_id is required/)
    end
  end

  describe "cleanup_old_transactions" do
    before(:each) do
      mock_redis
      # Setup entities
      CfdpMib.define_entity(1)
      CfdpMib.source_entity_id = 1
      CfdpMib.entity(1)['transaction_retain_seconds'] = 60

      # Create mock transactions
      @active_tx = double("Transaction")
      allow(@active_tx).to receive(:complete_time).and_return(nil)
      allow(@active_tx).to receive(:delete)

      @recent_tx = double("Transaction")
      allow(@recent_tx).to receive(:complete_time).and_return(Time.now.utc - 30)
      allow(@recent_tx).to receive(:delete)

      @old_tx = double("Transaction")
      allow(@old_tx).to receive(:complete_time).and_return(Time.now.utc - 120)
      allow(@old_tx).to receive(:delete)

      # Add transactions to the MIB
      CfdpMib.transactions["tx1"] = @active_tx
      CfdpMib.transactions["tx2"] = @recent_tx
      CfdpMib.transactions["tx3"] = @old_tx
    end

    it "calls delete on old completed transactions" do
      CfdpMib.cleanup_old_transactions

      # Verify only old transaction's delete method was called
      expect(@active_tx).not_to have_received(:delete)
      expect(@recent_tx).not_to have_received(:delete)
      expect(@old_tx).to have_received(:delete)
    end

    it "logs the number of cleaned up transactions" do
      expect(OpenC3::Logger).to receive(:info).with("CFDP cleaned up 1 completed transactions", scope: ENV['OPENC3_SCOPE'])
      CfdpMib.cleanup_old_transactions
    end

    it "skips transactions without complete_time" do
      CfdpMib.cleanup_old_transactions
      expect(@active_tx).not_to have_received(:delete)
    end

    it "skips recent completed transactions" do
      CfdpMib.cleanup_old_transactions
      expect(@recent_tx).not_to have_received(:delete)
    end

    it "uses transaction_retain_seconds from source entity" do
      # Set a longer retain period
      CfdpMib.entity(1)['transaction_retain_seconds'] = 200

      CfdpMib.cleanup_old_transactions

      # Now even the old transaction should not be deleted
      expect(@old_tx).not_to have_received(:delete)
    end

    it "handles multiple old transactions" do
      # Add another old transaction
      old_tx2 = double("Transaction")
      allow(old_tx2).to receive(:complete_time).and_return(Time.now.utc - 150)
      allow(old_tx2).to receive(:delete)
      CfdpMib.transactions["tx4"] = old_tx2

      expect(OpenC3::Logger).to receive(:info).with("CFDP cleaned up 2 completed transactions", scope: ENV['OPENC3_SCOPE'])

      CfdpMib.cleanup_old_transactions

      expect(@old_tx).to have_received(:delete)
      expect(old_tx2).to have_received(:delete)
    end
  end

  describe "clear" do
    it "clears all saved transaction states" do
      mock_redis
      allow(CfdpTransaction).to receive(:clear_saved_transaction_ids).and_call_original

      OpenC3::Store.sadd("#{@redis_prefix}cfdp_saved_transaction_ids", "tx1")
      OpenC3::Store.sadd("#{@redis_prefix}cfdp_saved_transaction_ids", "tx2")
      OpenC3::Store.hset("#{@redis_prefix}cfdp_transaction_state:tx1", "id", "tx1")
      OpenC3::Store.hset("#{@redis_prefix}cfdp_transaction_state:tx2", "id", "tx2")
      expect(CfdpTransaction.get_saved_transaction_ids.length).to eq(2)

      CfdpMib.clear
      expect(CfdpTransaction.get_saved_transaction_ids).to be_empty
    end
  end

  describe "load_saved_transactions" do
    before(:each) do
      mock_redis
      allow(OpenC3::Logger).to receive(:info)
      allow(OpenC3::Logger).to receive(:warn)
      allow(OpenC3::Logger).to receive(:error)

      # Setup entities for testing
      CfdpMib.define_entity(1)
      CfdpMib.define_entity(2)
      CfdpMib.source_entity_id = 1
    end

    it "loads no transactions when none are saved" do
      expect(CfdpTransaction).to receive(:get_saved_transaction_ids).and_return([])

      CfdpMib.load_saved_transactions

      expect(CfdpMib.transactions).to be_empty
      expect(OpenC3::Logger).to have_received(:info).with("CFDP loaded 0 saved transactions", scope: 'DEFAULT')
    end

    it "loads a source transaction successfully" do
      # Mock saved transaction ID for source transaction
      saved_ids = ["1__123"]
      expect(CfdpTransaction).to receive(:get_saved_transaction_ids).and_return(saved_ids)

      # Mock the CfdpSourceTransaction creation and loading
      mock_transaction = double("CfdpSourceTransaction")
      allow(mock_transaction).to receive(:load_state).with("1__123").and_return(true)
      expect(CfdpSourceTransaction).to receive(:new).with(source_entity: CfdpMib.entity(1)) do |args|
        # Simulate the constructor behavior of adding to transactions hash
        CfdpMib.transactions["1__123"] = mock_transaction
        mock_transaction
      end

      CfdpMib.load_saved_transactions

      expect(CfdpMib.transactions["1__123"]).to eq(mock_transaction)
      expect(OpenC3::Logger).to have_received(:info).with("CFDP loaded 1 saved transactions", scope: 'DEFAULT')
    end

    it "loads a receive transaction successfully" do
      # Mock saved transaction ID for receive transaction (different source entity)
      saved_ids = ["2__456"]
      expect(CfdpTransaction).to receive(:get_saved_transaction_ids).and_return(saved_ids)

      # Mock the CfdpReceiveTransaction creation and loading
      mock_transaction = double("CfdpReceiveTransaction")
      allow(mock_transaction).to receive(:load_state).with("2__456").and_return(true)
      expected_pdu = {
        "SOURCE_ENTITY_ID" => 2,
        "SEQUENCE_NUMBER" => 456,
        "TRANSMISSION_MODE" => "UNACKNOWLEDGED"
      }
      expect(CfdpReceiveTransaction).to receive(:new).with(expected_pdu) do |args|
        # Simulate the constructor behavior of adding to transactions hash
        CfdpMib.transactions["2__456"] = mock_transaction
        mock_transaction
      end

      CfdpMib.load_saved_transactions

      expect(CfdpMib.transactions["2__456"]).to eq(mock_transaction)
      expect(OpenC3::Logger).to have_received(:info).with("CFDP loaded 1 saved transactions", scope: 'DEFAULT')
    end

    it "handles failed state loading gracefully" do
      saved_ids = ["1__789"]
      expect(CfdpTransaction).to receive(:get_saved_transaction_ids).and_return(saved_ids)

      # Mock transaction that fails to load state
      mock_transaction = double("CfdpSourceTransaction")
      allow(mock_transaction).to receive(:load_state).with("1__789").and_return(false)
      expect(CfdpSourceTransaction).to receive(:new).with(source_entity: CfdpMib.entity(1)).and_return(mock_transaction)

      CfdpMib.load_saved_transactions

      expect(CfdpMib.transactions).not_to have_key("1__789")
      expect(OpenC3::Logger).to have_received(:warn).with("CFDP failed to load saved transaction: 1__789", scope: 'DEFAULT')
      expect(OpenC3::Logger).to have_received(:info).with("CFDP loaded 0 saved transactions", scope: 'DEFAULT')
    end

    it "handles transaction creation errors and cleans up invalid state" do
      saved_ids = ["1__999"]
      expect(CfdpTransaction).to receive(:get_saved_transaction_ids).and_return(saved_ids)

      # Mock transaction creation that raises an error
      expect(CfdpSourceTransaction).to receive(:new).and_raise("Transaction creation failed")

      CfdpMib.load_saved_transactions

      expect(CfdpMib.transactions).not_to have_key("1__999")
      expect(OpenC3::Logger).to have_received(:error).with("CFDP error loading saved transaction 1__999: Transaction creation failed", scope: 'DEFAULT')

      # Verify cleanup of invalid state
      expect(OpenC3::Store.exists("#{@redis_prefix}cfdp_transaction_state:1__999")).to eq(0)
      expect(CfdpTransaction.has_saved_state?("1__999")).to be false
      expect(OpenC3::Logger).to have_received(:info).with("CFDP loaded 0 saved transactions", scope: 'DEFAULT')
    end

    it "loads multiple transactions of different types" do
      saved_ids = ["1__100", "2__200", "1__300"]
      expect(CfdpTransaction).to receive(:get_saved_transaction_ids).and_return(saved_ids)

      # Mock source transactions
      source_tx1 = double("CfdpSourceTransaction1")
      source_tx2 = double("CfdpSourceTransaction2")
      allow(source_tx1).to receive(:load_state).with("1__100").and_return(true)
      allow(source_tx2).to receive(:load_state).with("1__300").and_return(true)

      call_count = 0
      expect(CfdpSourceTransaction).to receive(:new).with(source_entity: CfdpMib.entity(1)).twice do |args|
        call_count += 1
        if call_count == 1
          # Simulate the constructor behavior of adding to transactions hash
          CfdpMib.transactions["1__100"] = source_tx1
          source_tx1
        else
          # Simulate the constructor behavior of adding to transactions hash
          CfdpMib.transactions["1__300"] = source_tx2
          source_tx2
        end
      end

      # Mock receive transaction
      receive_tx = double("CfdpReceiveTransaction")
      allow(receive_tx).to receive(:load_state).with("2__200").and_return(true)
      expected_pdu = {
        "SOURCE_ENTITY_ID" => 2,
        "SEQUENCE_NUMBER" => 200,
        "TRANSMISSION_MODE" => "UNACKNOWLEDGED"
      }
      expect(CfdpReceiveTransaction).to receive(:new).with(expected_pdu) do |args|
        # Simulate the constructor behavior of adding to transactions hash
        CfdpMib.transactions["2__200"] = receive_tx
        receive_tx
      end

      CfdpMib.load_saved_transactions

      expect(CfdpMib.transactions["1__100"]).to eq(source_tx1)
      expect(CfdpMib.transactions["2__200"]).to eq(receive_tx)
      expect(CfdpMib.transactions["1__300"]).to eq(source_tx2)
      expect(OpenC3::Logger).to have_received(:info).with("CFDP loaded 3 saved transactions", scope: 'DEFAULT')
    end
  end

  describe "setup with transaction loading" do
    before(:each) do
      mock_redis
      allow(OpenC3::Logger).to receive(:info)

      # Mock the MicroserviceModel
      @mock_model = double("MicroserviceModel")
      @options = [
        ["source_entity_id", "1"],
        ["destination_entity_id", "2"],
        ["root_path", "/tmp"]
      ]
      allow(@mock_model).to receive(:options).and_return(@options)
      allow(OpenC3::MicroserviceModel).to receive(:get_model).and_return(@mock_model)
    end

    it "calls load_saved_transactions after setup completion" do
      expect(CfdpMib).to receive(:load_saved_transactions)

      CfdpMib.setup
    end
  end
end

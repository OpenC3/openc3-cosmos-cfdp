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

# Table 8-2: Remote Entity Configuration Information
# Remote entity ID
# Protocol version number
# UT address
# Positive ACK timer interval
# NAK timer interval
# Keep Alive interval
# Immediate NAK mode enabled
# Default transmission mode
# Transaction closure requested
# Check limit
# Default type of checksum to calculate for all file transmission to this remote entity
# Disposition of incomplete received file on transaction cancellation
# CRCs required on transmission
# Maximum file segment length
# Keep Alive discrepancy limit
# Positive ACK timer expiration limit
# NAK timer expiration limit
# Transaction inactivity limit
# Start of transmission opportunity
# End of transmission opportunity
# Start of reception opportunity
# End of reception opportunity

require 'openc3/models/microservice_model'
require 'openc3/utilities/bucket'
require 'openc3/utilities/logger'
require 'tempfile'
require 'fileutils'
require 'json'

class Tempfile
  def persist(filename)
    FileUtils.mv(self.path, filename)
    ObjectSpace.undefine_finalizer(self)
  end
end

class CfdpMib
  KNOWN_FIELD_NAMES = [
    'protocol_version_number',
    'cmd_info',
    'tlm_info',
    'ack_timer_interval',
    'nak_timer_interval',
    'keep_alive_interval',
    'immediate_nak_mode',
    'default_transmission_mode',
    'transaction_closure_requested',
    'check_limit',
    'default_checksum_type',
    'incomplete_file_disposition',
    'crcs_required',
    'maximum_file_segment_length',
    'keep_alive_discrepancy_limit',
    'ack_timer_expiration_limit',
    'nak_timer_expiration_limit',
    'transaction_inactivity_limit',
    'entity_id_length',
    'sequence_number_length'
  ]

  @@source_entity_id = 0
  @@entities = {}
  @@bucket = nil
  @@root_path = "/"
  @@transactions = {}

  def self.transactions
    @@transactions
  end

  def self.entity(entity_id)
    return @@entities[entity_id]
  end

  def self.source_entity_id=(id)
    @@source_entity_id = id
  end

  def self.source_entity_id
    @@source_entity_id
  end

  def self.source_entity
    return @@entities[@@source_entity_id]
  end

  def self.bucket=(bucket)
    @@bucket = bucket
  end

  def self.bucket
    return @@bucket
  end

  def self.root_path=(root_path)
    @@root_path = root_path
  end

  def self.root_path
    @@root_path
  end

  def self.define_entity(entity_id)
    entity_id = Integer(entity_id)
    entity = {}
    entity['id'] = entity_id
    entity['protocol_version_number'] = 0
    # These two settings map to UT address in COSMOS
    entity['cmd_info'] = nil
    entity['tlm_info'] = []
    entity['ack_timer_interval'] = 600
    entity['nak_timer_interval'] = 600
    entity['keep_alive_interval'] = 600
    entity['check_interval'] = 600
    entity['immediate_nak_mode'] = true
    entity['default_transmission_mode'] = 'UNACKNOWLEDGED'
    entity['transaction_closure_requested'] = "CLOSURE_REQUESTED"
    entity['default_checksum_type'] = 0
    entity['incomplete_file_disposition'] = "DISCARD"
    entity['crcs_required'] = true
    entity['maximum_file_segment_length'] = 1024
    entity['keep_alive_discrepancy_limit'] = entity['maximum_file_segment_length'] * 1000
    entity['ack_timer_expiration_limit'] = 1
    entity['nak_timer_expiration_limit'] = 1
    entity['transaction_inactivity_limit'] = 1
    entity['check_limit'] = 1
    entity['entity_id_length'] = 0 # 0 = 1 byte
    entity['sequence_number_length'] = 0 # 0 = 1 byte
    # TODO: Use interface connected? to limit opportunities?
    @@entities[entity_id] = entity
    return entity
  end

  def self.set_entity_value(entity_id, field_name, value)
    field_name = field_name.downcase
    entity_id = Integer(entity_id)
    raise "Unknown OPTION #{field_name}" unless KNOWN_FIELD_NAMES.include?(field_name)
    case field_name
    when 'tlm_info'
      if value.length == 3
        @@entities[entity_id][field_name] << value
      else
        raise "Invalid tlm_info: #{value}"
      end
    when 'cmd_info'
      if value.length == 3
        @@entities[entity_id][field_name] = value
      else
        raise "Invalid cmd_info: #{value}"
      end
    else
      @@entities[entity_id][field_name] = value
    end
  end

  def self.get_source_file(source_file_name)
    file_name = File.join(@@root_path, source_file_name)
    if self.bucket
      file = Tempfile.new
      OpenC3::Bucket.getClient().get_object(bucket: self.bucket, key: file_name, path: file.path)
    else
      file = File.open(file_name, 'rb')
    end
    file
  rescue Errno::ENOENT => error
    OpenC3::Logger.error(error.message)
    nil
  end

  def self.complete_source_file(file)
    file.close
  end

  def self.put_destination_file(destination_filename, tmp_file)
    file_name = File.join(@@root_path, destination_filename)
    if self.bucket
      OpenC3::Bucket.getClient().put_object(bucket: self.bucket, key: file_name, body: tmp_file.open.read)
    else
      file_name = File.join(@@root_path, destination_filename)
      tmp_file.persist(file_name)
    end
    tmp_file.unlink
    return true
  rescue
    # Something went wrong so return false
    return false
  end

  def self.filestore_request(action_code, first_file_name, second_file_name)
    # Apply root path
    first_file_name = File.join(@@root_path, first_file_name.to_s)
    second_file_name = File.join(@@root_path, second_file_name.to_s) if second_file_name

    # Handle file path safety
    first_file_name = File.absolute_path(first_file_name)
    second_file_name = File.absolute_path(second_file_name) if second_file_name
    if (first_file_name.index(@@root_path) != 0) or (second_file_name and second_file_name.index(@@root_path) != 0)
      return "NOT_ALLOWED", "Dangerous filename"
    end

    status_code = nil
    filestore_message = nil
    begin
      case action_code
      when "CREATE_FILE"
        if self.bucket
          OpenC3::Bucket.getClient().put_object(bucket: self.bucket, key: first_file_name, body: '')
        else
          FileUtils.touch(first_file_name)
        end
        status_code = "SUCCESSFUL"

      when "DELETE_FILE"
        if self.bucket
          client = OpenC3::Bucket.getClient()
          if client.check_object(bucket: self.bucket, key: first_file_name)
            client.delete_object(bucket: self.bucket, key: first_file_name)
            status_code = "SUCCESSFUL"
          else
            status_code = "FILE_DOES_NOT_EXIST"
          end
        else
          if File.exist?(first_file_name)
            FileUtils.rm(first_file_name)
            status_code = "SUCCESSFUL"
          else
            status_code = "FILE_DOES_NOT_EXIST"
          end
        end

      when "RENAME_FILE"
        if self.bucket
          client = OpenC3::Bucket.getClient()
          if client.check_object(bucket: self.bucket, key: second_file_name)
            status_code = "NEW_FILE_ALREADY_EXISTS"
          elsif not client.check_object(bucket: self.bucket, key: first_file_name)
            status_code = "OLD_FILE_DOES_NOT_EXIST"
          else
            temp = Tempfile.new
            client.get_object(bucket: self.bucket, key: first_file_name, path: temp.path)
            client.put_object(bucket: self.bucket, key: second_file_name, body: temp.read)
            client.delete_object(bucket: self.bucket, key: first_file_name)
            temp.unlink
            status_code = "SUCCESSFUL"
          end
        else
          if File.exist?(second_file_name)
            status_code = "NEW_FILE_ALREADY_EXISTS"
          elsif not File.exist?(first_file_name)
            status_code = "OLD_FILE_DOES_NOT_EXIST"
          else
            FileUtils.mv(first_file_name, second_file_name)
            status_code = "SUCCESSFUL"
          end
        end

      when "APPEND_FILE"
        if self.bucket
          client = OpenC3::Bucket.getClient()
          if not client.check_object(bucket: self.bucket, key: first_file_name)
            status_code = "FILE_1_DOES_NOT_EXIST"
          elsif not client.check_object(bucket: self.bucket, key: second_file_name)
            status_code = "FILE_2_DOES_NOT_EXIST"
          else
            temp1 = Tempfile.new
            temp2 = Tempfile.new
            client.get_object(bucket: self.bucket, key: first_file_name, path: temp1.path)
            client.get_object(bucket: self.bucket, key: second_file_name, path: temp2.path)
            client.put_object(bucket: self.bucket, key: first_file_name, body: temp1.read + temp2.read)
            temp1.unlink
            temp2.unlink
            status_code = "SUCCESSFUL"
          end
        else
          if not File.exist?(first_file_name)
            status_code = "FILE_1_DOES_NOT_EXIST"
          elsif not File.exist?(second_file_name)
            status_code = "FILE_2_DOES_NOT_EXIST"
          else
            File.open(first_file_name, 'ab') do |file|
              file.write(File.read(second_file_name))
            end
            status_code = "SUCCESSFUL"
          end
        end

      when "REPLACE_FILE"
        if self.bucket
          client = OpenC3::Bucket.getClient()
          if not client.check_object(bucket: self.bucket, key: first_file_name)
            status_code = "FILE_1_DOES_NOT_EXIST"
          elsif not client.check_object(bucket: self.bucket, key: second_file_name)
            status_code = "FILE_2_DOES_NOT_EXIST"
          else
            temp = Tempfile.new
            client.get_object(bucket: self.bucket, key: second_file_name, path: temp.path)
            client.put_object(bucket: self.bucket, key: first_file_name, body: temp.read)
            temp.unlink
            status_code = "SUCCESSFUL"
          end
        else
          if not File.exist?(first_file_name)
            status_code = "FILE_1_DOES_NOT_EXIST"
          elsif not File.exist?(second_file_name)
            status_code = "FILE_2_DOES_NOT_EXIST"
          else
            File.open(first_file_name, 'wb') do |file|
              file.write(File.read(second_file_name))
            end
            status_code = "SUCCESSFUL"
          end
        end

      when "CREATE_DIRECTORY"
        # Creating a directory in a bucket doesn't make sense so it's a noop
        FileUtils.mkdir(first_file_name) unless self.bucket
        status_code = "SUCCESSFUL"

      when "REMOVE_DIRECTORY"
        if self.bucket
          # Stand alone directories don't make sense in buckets because
          # it's only files which are stored and the path is a string.
          # Thus we'll just always return SUCCESSFUL.
          status_code = "SUCCESSFUL"
        else
          if not Dir.exist?(first_file_name)
            status_code = "DOES_NOT_EXIST"
          else
            FileUtils.rmdir(first_file_name)
            status_code = "SUCCESSFUL"
          end
        end

      when "DENY_FILE"
        if self.bucket
          begin
            OpenC3::Bucket.getClient().delete_object(bucket: self.bucket, key: first_file_name)
          rescue
            # Don't care if the file doesn't exist
          end
          status_code = "SUCCESSFUL"
        else
          if File.exist?(first_file_name)
            FileUtils.rm(first_file_name)
            status_code = "SUCCESSFUL"
          else
            status_code = "SUCCESSFUL"
          end
        end

      when "DENY_DIRECTORY"
        if self.bucket
          # Stand alone directories don't make sense in buckets because
          # it's only files which are stored and the path is a string.
          # Thus we'll just always return SUCCESSFUL.
          status_code = "SUCCESSFUL"
        else
          if not Dir.exist?(first_file_name)
            status_code = "SUCCESSFUL"
          else
            FileUtils.rmdir(first_file_name)
            status_code = "SUCCESSFUL"
          end
        end

      else
        status_code = "NOT_PERFORMED"
        filestore_message = "Unknown action code: #{action_code}"
      end
    rescue => err
      if action_code != "CREATE_DIRECTORY"
        status_code = "NOT_ALLOWED"
      else
        status_code = "CANNOT_BE_CREATED"
      end
      filestore_message = "#{err.class}:#{err.message}"
    end

    return status_code, filestore_message
  end

  def self.setup
    # Get options for our microservice
    model = OpenC3::MicroserviceModel.get_model(name: ENV['OPENC3_MICROSERVICE_NAME'], scope: ENV['OPENC3_SCOPE'])

    # Initialize MIB from OPTIONS
    current_entity_id = nil
    source_entity_defined = false
    destination_entity_defined = false
    root_path_defined = false
    model.options.each do |option|
      field_name = option[0].to_s.downcase
      value = option[1..-1]
      value = value[0] if value.length == 1
      case field_name
      when 'source_entity_id'
        source_entity_defined = true
        current_entity_id = Integer(value)
        CfdpMib.define_entity(current_entity_id)
        CfdpMib.source_entity_id = current_entity_id
      when 'destination_entity_id'
        destination_entity_defined = true
        current_entity_id = Integer(value)
        CfdpMib.define_entity(current_entity_id)
      when 'bucket'
        CfdpMib.bucket = value
      when 'root_path'
        root_path_defined = true
        CfdpMib.root_path = value
      else
        if current_entity_id
          CfdpMib.set_entity_value(current_entity_id, field_name, value)
        else
          raise "Must declare source_entity_id or destination_entity_id before other options"
        end
      end
    end

    raise "OPTION source_entity_id is required" unless source_entity_defined
    raise "OPTION destination_entity_id is required" unless destination_entity_defined
    raise "OPTION root_path is required" unless root_path_defined
  end

  def self.directory_listing(directory_name, directory_file_name)
    # Apply root path
    directory_name = File.join(@@root_path, directory_name.to_s)
    directory_file_name = File.join(@@root_path, directory_file_name.to_s)

    # Handle file path safety
    directory_name = File.absolute_path(directory_name)
    directory_file_name = File.absolute_path(directory_file_name)
    if (directory_name.index(@@root_path) != 0) or (directory_file_name.index(@@root_path) != 0)
      return nil
    end

    result = []
    if self.bucket
      dirs, files = OpenC3::Bucket.getClient().list_files(bucket: self.bucket, path: directory_name)
      dirs.each do |dir|
        result << {"directory" => dir}
      end
      files.each do |file|
        result << file
      end
    else
      entries = Dir.entries(directory_name)
      entries.each do |entry|
        next if entry == '.' or entry == '..'
        full_name = File.join(directory_name, entry)
        if File.directory?(full_name)
          result << {"directory" => entry}
        else
          stat = File.stat(full_name)
          result << {"name" => entry, "modified" => stat.mtime.to_s, "size" => stat.size}
        end
      end
    end
    json_result = JSON.pretty_generate(result.as_json)
    return json_result
  end
end

# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

require 'rails_helper'
require 'openc3/script'
require 'openc3/api/api'
require 'openc3/models/microservice_model'
require 'openc3/utilities/store_autoload'
require 'openc3/topics/command_topic'

OpenC3.disable_warnings do
  # Load the json_rpc.rb to ensure it overrides anything Rails is doing with as_json
  load 'openc3/io/json_rpc.rb'
end

module OpenC3
  RSpec.describe "cfdp", type: :request do
    describe "POST /cfdp/put" do
      before(:each) do
        mock_redis()
      end

      def setup(source_id:, destination_id:)
        @source_entity_id = source_id
        @destination_entity_id = destination_id
        ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
        # Create the model that is consumed by CfdpMib.setup
        model = MicroserviceModel.new(name: ENV['OPENC3_MICROSERVICE_NAME'], scope: "DEFAULT",
          options: [
            ["source_entity_id", @source_entity_id],
            ["cmd_info", "CFDPTEST", "CFDP_PDU", "PDU"],
            ["tlm_info", "CFDPTEST", "CFDP_PDU", "PDU"],
            ["destination_entity_id", @destination_entity_id],
            ["cmd_info", "CFDPTEST", "CFDP_PDU", "PDU"],
            ["tlm_info", "CFDPTEST", "CFDP_PDU", "PDU"],
            ["root_path", SPEC_DIR],
          ],
        )
        model.create
        CfdpMib.setup

        @tx_pdus = []
        @rx_pdus = []
        @packets = []
        allow_any_instance_of(CfdpSourceTransaction).to receive('cmd') do |source, tgt_name, pkt_name, params|
          # puts params["PDU"].formatted # Prints the raw bytes
          @tx_pdus << CfdpPdu.decom(params["PDU"])
          @packets << [tgt_name, pkt_name, params]
        end
        allow_any_instance_of(CfdpReceiveTransaction).to receive('cmd') do |source, tgt_name, pkt_name, params|
          @rx_pdus << CfdpPdu.decom(params["PDU"])
        end
      end

      # Helper method to perform a filestore_request and return the indication
      def request(source: nil, dest: nil, requests: [], transmission_mode: 'UNACKNOWLEDGED',
                  closure: 'CLOSURE_NOT_REQUESTED', send_closure: true, cancel: false, skip: false)
        setup(source_id: 1, destination_id: 2)
        CfdpMib.set_entity_value(@destination_entity_id, 'maximum_file_segment_length', 8)
        CfdpMib.root_path = @root_path
        if @bucket
          CfdpMib.bucket = @bucket
        else
          CfdpMib.bucket = nil
        end

        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: source, destination_file_name: dest,
          filestore_requests: requests, closure_requested: closure,
          transmission_mode: transmission_mode
        }, as: :json
        expect(response).to have_http_status(200)
        sleep 0.1

        # Start user thread here so it has the change to receive the closure PDU
        @user = CfdpUser.new
        @user.start
        sleep 0.1 # Allow user thread to start

        if closure == 'CLOSURE_REQUESTED' or transmission_mode == 'ACKNOWLEDGED'
          if send_closure
            # Simlulate the finished PDU
            cmd_params = {}
            cmd_params["PDU"] = CfdpPdu.build_finished_pdu(
              source_entity: CfdpMib.entity(@source_entity_id),
              transaction_seq_num: 1,
              destination_entity: CfdpMib.entity(@destination_entity_id),
              condition_code: "NO_ERROR",
              delivery_code: "DATA_COMPLETE",
              file_status: "FILESTORE_SUCCESS",
            )
            msg_hash = {
              :time => Time.now.to_nsec_from_epoch,
              :stored => 'false',
              :target_name => "CFDPTEST",
              :packet_name => "CFDP_PDU",
              :received_count => 1,
              :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
            }
            Topic.write_topic("DEFAULT__DECOM__{CFDPTEST}__CFDP_PDU", msg_hash, nil)
          end
          sleep 1.1
        end

        # Clear the tx transactions to simulate the receive side on the same system
        keys = CfdpMib.transactions.keys
        keys.each do |key|
          CfdpMib.transactions.delete(key)
        end

        i = -1
        @packets.each do |target_name, cmd_name, cmd_params|
          i += 1
          # Skip is an array of segments to skip (1 based)
          next if skip and skip.include?(i)
          msg_hash = {
            :time => Time.now.to_nsec_from_epoch,
            :stored => 'false',
            :target_name => target_name,
            :packet_name => cmd_name,
            :received_count => 1,
            :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
          }
          Topic.write_topic("DEFAULT__DECOM__{#{target_name}}__#{cmd_name}", msg_hash, nil)
          if i == 1
            if cancel
              post "/cfdp/cancel", :params => {
                scope: "DEFAULT", transaction_id: "#{@source_entity_id}__1"
              }, as: :json
            end
          end
        end
        sleep 0.1
        @user.stop
        sleep 0.1

        get "/cfdp/indications", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(200)
        json = JSON.parse(response.body)
        yield json['indications']
      end

      it "requires a destination_entity_id" do
        setup(source_id: 10, destination_id: 20)
        post "/cfdp/put", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(400)
        expect(response.body).to match(/missing.*destination_entity_id/)
      end

      it "requires a numeric destination_entity_id" do
        setup(source_id: 10, destination_id: 20)
        post "/cfdp/put", :params => { scope: "DEFAULT", destination_entity_id: "HI" }
        expect(response).to have_http_status(400)
      end

      %w(local bucket).each do |type|
        context "with #{type} filestore requests" do
          if type == 'bucket'
            # Enable if there's an actual MINIO service avaiable to talk to
            # To enable access to MINIO for testing change the compose.yaml file and add
            # the following to services: open3-minio:
            #   ports:
            #     - "127.0.0.1:9000:9000"
            if ENV['MINIO']
              before(:all) do
                @bucket = OpenC3::Bucket.getClient.create("bucket#{rand(1000)}")
                @root_path = 'path'
              end
              after(:all) do
                OpenC3::Bucket.getClient.delete(@bucket) if @bucket
              end
            else
              # Simulate the bucket by stubbing out the bucket client
              before(:each) do
                @client = double("getClient").as_null_object
                allow(@client).to receive(:exist?).and_return(true)
                allow(@client).to receive(:get_object) do |bucket:, key:, path:|
                  File.write(path, File.read(key))
                end
                allow(@client).to receive(:put_object) do |bucket:, key:, body:|
                  File.write(key, body)
                end
                allow(@client).to receive(:check_object) do |bucket:, key:|
                  File.exist?(key)
                end
                allow(@client).to receive(:delete_object) do |bucket:, key:|
                  FileUtils.rm(key)
                end
                allow(OpenC3::Bucket).to receive(:getClient).and_return(@client)
                @root_path = SPEC_DIR
                @bucket = 'config'
              end
            end
          else
            before(:each) do
              @root_path = SPEC_DIR
            end
          end

          it "sends a text file" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt') do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'UNREPORTED'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '8'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'EOF-Recv'
              expect(indications[6]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[6]['condition_code']).to eql 'NO_ERROR'
              expect(indications[6]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[6]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[6]['status_report']).to eql 'FINISHED'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')

            # Validate the PDUs
            expect(@tx_pdus.length).to eql 3
            expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
            expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

            expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[1]['FILE_DATA']).to eql data

            expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
            expect(@tx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
          end

          it "handles a failed checksum" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            expect_any_instance_of(CfdpChecksum).to receive(:check).and_return(false)
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt') do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'UNREPORTED'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '8'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'EOF-Recv'
              expect(indications[6]['indication_type']).to eql 'Fault'
              expect(indications[6]['condition_code']).to eql 'FILE_CHECKSUM_FAILURE'
              expect(indications[6]['progress']).to eql '8'
              expect(indications[7]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[7]['condition_code']).to eql 'FILE_CHECKSUM_FAILURE'
              expect(indications[7]['file_status']).to eql 'FILE_DISCARDED'
              expect(indications[7]['delivery_code']).to eql 'DATA_INCOMPLETE'
              expect(indications[7]['status_report']).to eql 'FINISHED'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true

            # Validate the PDUs
            expect(@tx_pdus.length).to eql 3
            expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
            expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

            expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[1]['FILE_DATA']).to eql data

            expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
            expect(@tx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
          end

          it "sends data acknowledged" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt',
                    transmission_mode: 'ACKNOWLEDGED') do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '8'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'EOF-Recv'
              expect(indications[6]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[6]['condition_code']).to eql 'NO_ERROR'
              expect(indications[6]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[6]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[6]['status_report']).to eql 'FINISHED'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')

            # Validate the Tx PDUs
            expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
            expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length
            expect(@tx_pdus[0]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'

            expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[1]['FILE_DATA']).to eql data
            expect(@tx_pdus[1]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'

            expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
            expect(@tx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
            expect(@tx_pdus[2]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'

            expect(@tx_pdus[3]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[3]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[3]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[3]['DIRECTIVE_CODE']).to eql 'ACK'
            expect(@tx_pdus[3]['ACK_DIRECTIVE_CODE']).to eql 'FINISHED'
            expect(@tx_pdus[3]['ACK_DIRECTIVE_SUBTYPE']).to eql 1
            expect(@tx_pdus[3]['CONDITION_CODE']).to eql 'NO_ERROR'
            expect(@tx_pdus[3]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
            expect(@tx_pdus[3]['TRANSACTION_STATUS']).to eql 'ACTIVE' # TODO: ACTIVE?

            # Validate the Rx PDUs
            expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @source_entity_id # Sent to ourselves
            expect(@rx_pdus[0]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
            expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'ACK'
            expect(@rx_pdus[0]['ACK_DIRECTIVE_CODE']).to eql 'EOF'
            expect(@rx_pdus[0]['ACK_DIRECTIVE_SUBTYPE']).to eql 0
            expect(@rx_pdus[0]['CONDITION_CODE']).to eql 'NO_ERROR'

            expect(@rx_pdus[1]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@rx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@rx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @source_entity_id
            expect(@rx_pdus[1]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
            expect(@rx_pdus[1]['DIRECTIVE_CODE']).to eql 'FINISHED'
            expect(@rx_pdus[1]['CONDITION_CODE']).to eql 'NO_ERROR'
            expect(@rx_pdus[1]['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
            expect(@rx_pdus[1]['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'
          end

          it "creates multiple segments" do
            data = ('a'..'z').to_a.shuffle[0,9].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt') do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'UNREPORTED'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '9'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[5]['offset']).to eql '8'
              expect(indications[5]['length']).to eql '1'
              expect(indications[6]['indication_type']).to eql 'EOF-Recv'
              expect(indications[7]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[7]['condition_code']).to eql 'NO_ERROR'
              expect(indications[7]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[7]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[7]['status_report']).to eql 'FINISHED'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')

            # Validate the PDUs
            expect(@tx_pdus.length).to eql 4
            expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
            expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

            expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[1]['FILE_DATA']).to eql data[0..7]

            expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[2]['FILE_DATA']).to eql data[8..-1]

            expect(@tx_pdus[3]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[3]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[3]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[3]['DIRECTIVE_CODE']).to eql 'EOF'
            expect(@tx_pdus[3]['CONDITION_CODE']).to eql 'NO_ERROR'
          end

          %w(1 2 3 1_2 2_3 1_3).each do |missing|
            context "with segment #{missing} missing" do
              it "sends a NAK" do
                data = ('a'..'z').to_a.shuffle[0,17].join
                File.write(File.join(SPEC_DIR, 'test1.txt'), data)
                segments = missing.split('_')
                segments.map! {|segment| segment.to_i }
                start_offset = nil
                end_offset = nil
                if segments.include?(3)
                  start_offset = 16
                end
                if segments.include?(2)
                  start_offset = 8
                end
                if segments.include?(1)
                  start_offset = 0
                  end_offset = 8
                end
                if segments.include?(2)
                  end_offset = 16
                end
                if segments.include?(3)
                  end_offset = 17
                end
                request(source: 'test1.txt', dest: 'test2.txt', skip: segments) do |indications|
                  # First the transmit indications
                  expect(indications[0]['indication_type']).to eql 'Transaction'
                  expect(indications[1]['indication_type']).to eql 'EOF-Sent'
                  expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
                  expect(indications[2]['condition_code']).to eql 'NO_ERROR'
                  expect(indications[2]['file_status']).to eql 'UNREPORTED'
                  expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
                  expect(indications[2]['status_report']).to eql 'FINISHED'
                  # Receive indications
                  expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
                  expect(indications[3]['source_file_name']).to eql 'test1.txt'
                  expect(indications[3]['destination_file_name']).to eql 'test2.txt'
                  expect(indications[3]['file_size']).to eql '17'
                  expect(indications[3]['source_entity_id']).to eql '1'
                  # Segment indications
                  i = 4
                  unless segments.include?(1)
                    expect(indications[i]['indication_type']).to eql 'File-Segment-Recv'
                    expect(indications[i]['offset']).to eql '0'
                    expect(indications[i]['length']).to eql '8'
                    i += 1
                  end
                  unless segments.include?(2)
                    expect(indications[i]['indication_type']).to eql 'File-Segment-Recv'
                    expect(indications[i]['offset']).to eql '8'
                    expect(indications[i]['length']).to eql '8'
                    i += 1
                  end
                  unless segments.include?(3)
                    expect(indications[i]['indication_type']).to eql 'File-Segment-Recv'
                    expect(indications[i]['offset']).to eql '16'
                    expect(indications[i]['length']).to eql '1'
                    i += 1
                  end
                  expect(indications[i]['indication_type']).to eql 'EOF-Recv'
                end
                expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
                FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true

                # Validate the TX PDUs
                expect(@tx_pdus.length).to eql 5
                expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
                expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
                expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
                expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
                expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

                # Even though a FILE_DATA pdu is skipped by receive it is still sent
                expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
                expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
                expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
                expect(@tx_pdus[1]['FILE_DATA']).to eql data[0..7]

                expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DATA'
                expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
                expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
                expect(@tx_pdus[2]['FILE_DATA']).to eql data[8..15]

                expect(@tx_pdus[3]['TYPE']).to eql 'FILE_DATA'
                expect(@tx_pdus[3]['SOURCE_ENTITY_ID']).to eql @source_entity_id
                expect(@tx_pdus[3]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
                expect(@tx_pdus[3]['FILE_DATA']).to eql data[16..-1]

                expect(@tx_pdus[4]['TYPE']).to eql 'FILE_DIRECTIVE'
                expect(@tx_pdus[4]['SOURCE_ENTITY_ID']).to eql @source_entity_id
                expect(@tx_pdus[4]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
                expect(@tx_pdus[4]['DIRECTIVE_CODE']).to eql 'EOF'
                expect(@tx_pdus[4]['CONDITION_CODE']).to eql 'NO_ERROR'

                # Validate the RX PDUs
                expect(@rx_pdus.length).to eql 1
                expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
                expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
                expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @source_entity_id # Sent to ourselves
                expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'NAK'
                expect(@rx_pdus[0]['START_OF_SCOPE']).to eql 0
                expect(@rx_pdus[0]['END_OF_SCOPE']).to eql 17
                # Section 4.6.4.3.3 b indicates the segment requests should indicate only the segments not yet received
                if segments == [1,3] # Special case
                  expect(@rx_pdus[0]['SEGMENT_REQUESTS']).to eql [{"START_OFFSET"=>0, "END_OFFSET"=>8}, {"START_OFFSET"=>16, "END_OFFSET"=>17}]
                else
                  expect(@rx_pdus[0]['SEGMENT_REQUESTS']).to eql [{"START_OFFSET"=>start_offset, "END_OFFSET"=>end_offset}]
                end
              end
            end
          end

          it "waits for a closure" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt', closure: 'CLOSURE_REQUESTED') do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '8'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'EOF-Recv'
              expect(indications[6]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[6]['condition_code']).to eql 'NO_ERROR'
              expect(indications[6]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[6]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[6]['status_report']).to eql 'FINISHED'
            end
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')

            # Validate the PDUs
            expect(@tx_pdus.length).to eql 3
            expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
            expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

            expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[1]['FILE_DATA']).to eql data[0..7]

            expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
            expect(@tx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'

            expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @source_entity_id # Sent to ourselves
            expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'FINISHED'
            expect(@rx_pdus[0]['CONDITION_CODE']).to eql 'NO_ERROR'
            expect(@rx_pdus[0]['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
            expect(@rx_pdus[0]['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'
          end

          it "times out waiting for a closure" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt', closure: 'CLOSURE_REQUESTED', send_closure: false) do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Fault'
              expect(indications[3]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[3]['condition_code']).to eql 'CHECK_LIMIT_REACHED'
              expect(indications[3]['file_status']).to eql 'UNREPORTED'
              expect(indications[3]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[3]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[4]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[4]['source_file_name']).to eql 'test1.txt'
              expect(indications[4]['destination_file_name']).to eql 'test2.txt'
              expect(indications[4]['file_size']).to eql '8'
              expect(indications[4]['source_entity_id']).to eql '1'
              expect(indications[5]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[5]['offset']).to eql '0'
              expect(indications[5]['length']).to eql '8'
              expect(indications[6]['indication_type']).to eql 'EOF-Recv'
              expect(indications[7]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[7]['condition_code']).to eql 'NO_ERROR'
              expect(indications[7]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[7]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[7]['status_report']).to eql 'FINISHED'
            end
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')
          end

          it "handles bad transaction IDs" do
            setup(source_id: 11, destination_id: 22)

            @user = CfdpUser.new
            thread = @user.start
            sleep 0.1 # Allow user thread to start
            expect(thread.alive?).to be true

            cmd_params = {}
            cmd_params["PDU"] = CfdpPdu.build_eof_pdu(
              source_entity: CfdpMib.entity(@source_entity_id),
              transaction_seq_num: 1,
              destination_entity: CfdpMib.entity(@destination_entity_id),
              file_size: 8,
              file_checksum: 0,
              condition_code: "NO_ERROR",
              segmentation_control: "NOT_PRESERVED",
              transmission_mode: nil,
              canceling_entity_id: nil)
            msg_hash = {
              :time => Time.now.to_nsec_from_epoch,
              :stored => 'false',
              :target_name => "CFDPTEST",
              :packet_name => "CFDP_PDU",
              :received_count => 1,
              :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
            }
            error_message = ''
            allow(OpenC3::Logger).to receive(:error) do |msg|
              error_message = msg
            end

            Topic.write_topic("DEFAULT__DECOM__{CFDPTEST}__CFDP_PDU", msg_hash, nil)
            sleep 0.1
            # Thread is still running even after the bad transaction
            expect(thread.alive?).to be true
            expect(error_message).to include("Unknown transaction")
            @user.stop
            sleep 0.1
          end

          it "cancels a transaction" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            # This cancels the receive transaction ... how to cancel the source transaction?
            request(source: 'test1.txt', dest: 'test2.txt', cancel: true) do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'UNREPORTED'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[-1]['condition_code']).to eql 'CANCEL_REQUEST_RECEIVED'
              expect(indications[-1]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[-1]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[-1]['status_report']).to eql 'CANCELED'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')

            # Validate the PDUs
            expect(@tx_pdus.length).to eql 3
            expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
            expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

            expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
            expect(@tx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[1]['FILE_DATA']).to eql data

            expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@tx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@tx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
            expect(@tx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
          end

          it "runs filestore requests after copy" do
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt',
              requests: [
                ['CREATE_FILE', 'new_file.txt']
              ]) do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'UNREPORTED'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '8'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'EOF-Recv'
              expect(indications[6]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[6]['condition_code']).to eql 'NO_ERROR'
              expect(indications[6]['file_status']).to eql 'FILESTORE_SUCCESS'
              expect(indications[6]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[6]['status_report']).to eql 'FINISHED'
              fsr = indications[6]['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'new_file.txt'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
            expect(File.exist?(File.join(SPEC_DIR, 'new_file.txt'))).to be true
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true # force ignores error if file doesn't exist
            FileUtils.rm File.join(SPEC_DIR, 'test2.txt')
            FileUtils.rm File.join(SPEC_DIR, 'new_file.txt')
          end

          it "skips filestore requests if copy fails" do
            # Simulate a failure
            allow(CfdpMib).to receive(:put_destination_file).and_return(false)
            data = ('a'..'z').to_a.shuffle[0,8].join
            File.write(File.join(SPEC_DIR, 'test1.txt'), data)
            request(source: 'test1.txt', dest: 'test2.txt',
              requests: [
                ['CREATE_FILE', 'new_file.txt']
              ]) do |indications|
              # First the transmit indications
              expect(indications[0]['indication_type']).to eql 'Transaction'
              expect(indications[1]['indication_type']).to eql 'EOF-Sent'
              expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[2]['condition_code']).to eql 'NO_ERROR'
              expect(indications[2]['file_status']).to eql 'UNREPORTED'
              expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[2]['status_report']).to eql 'FINISHED'
              # Receive indications
              expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
              expect(indications[3]['source_file_name']).to eql 'test1.txt'
              expect(indications[3]['destination_file_name']).to eql 'test2.txt'
              expect(indications[3]['file_size']).to eql '8'
              expect(indications[3]['source_entity_id']).to eql '1'
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql '0'
              expect(indications[4]['length']).to eql '8'
              expect(indications[5]['indication_type']).to eql 'EOF-Recv'
              expect(indications[6]['indication_type']).to eql 'Fault'
              expect(indications[6]['condition_code']).to eql 'FILESTORE_REJECTION'
              expect(indications[7]['indication_type']).to eql 'Transaction-Finished'
              expect(indications[7]['condition_code']).to eql 'FILESTORE_REJECTION'
              expect(indications[7]['file_status']).to eql 'FILESTORE_REJECTION'
              expect(indications[7]['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indications[7]['status_report']).to eql 'FINISHED'
              expect(indications[7]['filestore_responses']).to be nil
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
            expect(File.exist?(File.join(SPEC_DIR, 'new_file.txt'))).to be false
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
          end

          it "create file" do
            request(requests: [
              ['CREATE_FILE', "create_file.txt"],
              ['CREATE_FILE', "../nope"], # Outside of the root path
              ['CREATE_FILE', "another_file.txt"],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'create_file.txt'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'NOT_ALLOWED'
              expect(fsr['FIRST_FILE_NAME']).to eql '../nope'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              # Once there is a failure no more are performed per 4.9.5
              expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              expect(fsr['FIRST_FILE_NAME']).to eql 'another_file.txt'
            end
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'create_file.txt'))
            else
              FileUtils.rm File.join(SPEC_DIR, 'create_file.txt') # cleanup
            end
          end

          it "delete file" do
            request(requests: [
              ['CREATE_FILE', 'delete_file.txt'],
              ['DELETE_FILE', 'delete_file.txt'],
              ['DELETE_FILE', 'nope'],
              ['DELETE_FILE', 'another'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'delete_file.txt'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'DELETE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'delete_file.txt'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'DELETE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'FILE_DOES_NOT_EXIST'
              expect(fsr['FIRST_FILE_NAME']).to eql 'nope'
              fsr = indication['filestore_responses'][3]
              expect(fsr['ACTION_CODE']).to eql 'DELETE_FILE'
              # Once there is a failure no more are performed per 4.9.5
              expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              expect(fsr['FIRST_FILE_NAME']).to eql 'another'
            end
            if ENV['MINIO'] && type == 'bucket'
              expect(OpenC3::Bucket.getClient().check_object(bucket: @bucket, key: File.join(@root_path, 'delete_file.txt'))).to be false
            else
              expect(File.exist?(File.join(SPEC_DIR, 'delete_file.txt'))).to be false
            end
          end

          it "rename file" do
            request(requests: [
              ['CREATE_FILE', 'rename_file.txt'],
              ['RENAME_FILE', 'rename_file.txt', 'new_file.txt'],
              ['RENAME_FILE', 'nope', 'whatever'],
              ['RENAME_FILE', 'another'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'rename_file.txt'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'RENAME_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'rename_file.txt'
              expect(fsr['SECOND_FILE_NAME']).to eql 'new_file.txt'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'RENAME_FILE'
              expect(fsr['STATUS_CODE']).to eql 'OLD_FILE_DOES_NOT_EXIST'
              expect(fsr['FIRST_FILE_NAME']).to eql 'nope'
              expect(fsr['SECOND_FILE_NAME']).to eql 'whatever'
              fsr = indication['filestore_responses'][3]
              expect(fsr['ACTION_CODE']).to eql 'RENAME_FILE'
              # Once there is a failure no more are performed per 4.9.5
              expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              expect(fsr['FIRST_FILE_NAME']).to eql 'another'
            end
            if ENV['MINIO'] && type == 'bucket'
              expect(OpenC3::Bucket.getClient().check_object(bucket: @bucket, key: File.join(@root_path, 'rename_file.txt'))).to be false
              expect(OpenC3::Bucket.getClient().check_object(bucket: @bucket, key: File.join(@root_path, 'new_file.txt'))).to be true
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'new_file.txt'))
            else
              expect(File.exist?(File.join(SPEC_DIR, 'rename_file.txt'))).to be false
              expect(File.exist?(File.join(SPEC_DIR, 'new_file.txt'))).to be true
              FileUtils.rm File.join(SPEC_DIR, 'new_file.txt')
            end
          end

          it "rename file error" do
            request(requests: [
              ['CREATE_FILE', 'rename_file.txt'],
              ['RENAME_FILE', 'rename_file.txt', 'rename_file.txt'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'rename_file.txt'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'RENAME_FILE'
              expect(fsr['STATUS_CODE']).to eql 'NEW_FILE_ALREADY_EXISTS'
              expect(fsr['FIRST_FILE_NAME']).to eql 'rename_file.txt'
              expect(fsr['SECOND_FILE_NAME']).to eql 'rename_file.txt'
            end
            if ENV['MINIO'] && type == 'bucket'
              expect(OpenC3::Bucket.getClient().check_object(bucket: @bucket, key: File.join(@root_path, 'rename_file.txt'))).to be true
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'rename_file.txt'))
            else
              expect(File.exist?(File.join(SPEC_DIR, 'rename_file.txt'))).to be true
              FileUtils.rm File.join(SPEC_DIR, 'rename_file.txt')
            end
          end

          it "append file" do
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'first.txt'), body: 'FIRST')
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'second.txt'), body: 'SECOND')
            else
              File.write(File.join(SPEC_DIR, 'first.txt'), 'FIRST')
              File.write(File.join(SPEC_DIR, 'second.txt'), 'SECOND')
            end
            request(requests: [
              ['APPEND_FILE', 'first.txt', 'second.txt'],
              ['APPEND_FILE', 'nope', 'second.txt'],
              ['APPEND_FILE', 'another'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'APPEND_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'first.txt'
              expect(fsr['SECOND_FILE_NAME']).to eql 'second.txt'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'APPEND_FILE'
              expect(fsr['STATUS_CODE']).to eql 'FILE_1_DOES_NOT_EXIST'
              expect(fsr['FIRST_FILE_NAME']).to eql 'nope'
              expect(fsr['SECOND_FILE_NAME']).to eql 'second.txt'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'APPEND_FILE'
              # Once there is a failure no more are performed per 4.9.5
              expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              expect(fsr['FIRST_FILE_NAME']).to eql 'another'
            end
            if ENV['MINIO'] && type == 'bucket'
              file = Tempfile.new
              OpenC3::Bucket.getClient().get_object(bucket: @bucket, key: File.join(@root_path, 'first.txt'), path: file.path)
              expect(file.read).to eql 'FIRSTSECOND'
              file.unlink
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'first.txt'))
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'second.txt'))
            else
              expect(File.read(File.join(SPEC_DIR, 'first.txt'))).to eql 'FIRSTSECOND'
              FileUtils.rm File.join(SPEC_DIR, 'first.txt')
              FileUtils.rm File.join(SPEC_DIR, 'second.txt')
            end
          end

          it "append file error" do
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'first.txt'), body: 'FIRST')
            else
              File.write(File.join(SPEC_DIR, 'first.txt'), 'FIRST')
            end
            request(requests: [
              ['APPEND_FILE', 'first.txt', 'second.txt'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'APPEND_FILE'
              expect(fsr['STATUS_CODE']).to eql 'FILE_2_DOES_NOT_EXIST'
              expect(fsr['FIRST_FILE_NAME']).to eql 'first.txt'
              expect(fsr['SECOND_FILE_NAME']).to eql 'second.txt'
            end
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'first.txt'))
            else
              FileUtils.rm File.join(SPEC_DIR, 'first.txt')
            end
          end

          it "replace file" do
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'orig.txt'), body: 'ORIG')
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'replace.txt'), body: 'REPLACE')
            else
              File.write(File.join(SPEC_DIR, 'orig.txt'), 'ORIG')
              File.write(File.join(SPEC_DIR, 'replace.txt'), 'REPLACE')
            end
            request(requests: [
              ['REPLACE_FILE', 'orig.txt', 'replace.txt'],
              ['REPLACE_FILE', 'nope', 'replace.txt'],
              ['REPLACE_FILE', 'another'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'REPLACE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'orig.txt'
              expect(fsr['SECOND_FILE_NAME']).to eql 'replace.txt'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'REPLACE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'FILE_1_DOES_NOT_EXIST'
              expect(fsr['FIRST_FILE_NAME']).to eql 'nope'
              expect(fsr['SECOND_FILE_NAME']).to eql 'replace.txt'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'REPLACE_FILE'
              # Once there is a failure no more are performed per 4.9.5
              expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              expect(fsr['FIRST_FILE_NAME']).to eql 'another'
            end
            if ENV['MINIO'] && type == 'bucket'
              file = Tempfile.new
              OpenC3::Bucket.getClient().get_object(bucket: @bucket, key: File.join(@root_path, 'orig.txt'), path: file.path)
              expect(file.read).to eql 'REPLACE'
              file.rewind
              OpenC3::Bucket.getClient().get_object(bucket: @bucket, key: File.join(@root_path, 'replace.txt'), path: file.path)
              expect(file.read).to eql 'REPLACE'
              file.unlink
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'orig.txt'))
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'replace.txt'))
            else
              expect(File.read(File.join(SPEC_DIR, 'orig.txt'))).to eql 'REPLACE'
              expect(File.read(File.join(SPEC_DIR, 'replace.txt'))).to eql 'REPLACE' # Still exists
              FileUtils.rm File.join(SPEC_DIR, 'orig.txt')
              FileUtils.rm File.join(SPEC_DIR, 'replace.txt')
            end
          end

          it "replace file error" do
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'orig.txt'), body: 'ORIG')
            else
              File.write(File.join(SPEC_DIR, 'orig.txt'), 'ORIG')
            end
            request(requests: [
              ['REPLACE_FILE', 'orig.txt', 'replace.txt'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'REPLACE_FILE'
              expect(fsr['STATUS_CODE']).to eql 'FILE_2_DOES_NOT_EXIST'
              expect(fsr['FIRST_FILE_NAME']).to eql 'orig.txt'
              expect(fsr['SECOND_FILE_NAME']).to eql 'replace.txt'
            end
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().delete_object(bucket: @bucket, key: File.join(@root_path, 'orig.txt'))
            else
              FileUtils.rm File.join(SPEC_DIR, 'orig.txt')
            end
          end

          it "create directory" do
            request(requests: [
              ['CREATE_DIRECTORY', 'new_dir'],
              ['CREATE_DIRECTORY', 'new_dir'],
              ['CREATE_DIRECTORY', 'another_dir'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_DIRECTORY'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'new_dir'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_DIRECTORY'
              if type == 'bucket'
                expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              else
                expect(fsr['STATUS_CODE']).to eql 'CANNOT_BE_CREATED' # already exists
              end
              expect(fsr['FIRST_FILE_NAME']).to eql 'new_dir'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_DIRECTORY'
              if type == 'bucket'
                expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              else
                # Once there is a failure no more are performed per 4.9.5
                expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              end
              expect(fsr['FIRST_FILE_NAME']).to eql 'another_dir'
            end
            if type != 'bucket'
              expect(File.directory?(File.join(SPEC_DIR, 'new_dir'))).to be true
              FileUtils.rmdir File.join(SPEC_DIR, 'new_dir')
            end
          end

          it "remove directory" do
            request(requests: [
              ['CREATE_DIRECTORY', 'rm_dir'],
              ['REMOVE_DIRECTORY', 'rm_dir'],
              ['REMOVE_DIRECTORY', 'rm_dir'], # No longer exists
              ['REMOVE_DIRECTORY', 'another_dir'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'CREATE_DIRECTORY'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'rm_dir'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'REMOVE_DIRECTORY'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'rm_dir'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'REMOVE_DIRECTORY'
              if type == 'bucket'
                expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              else
                expect(fsr['STATUS_CODE']).to eql 'DOES_NOT_EXIST'
              end
              expect(fsr['FIRST_FILE_NAME']).to eql 'rm_dir'
              fsr = indication['filestore_responses'][3]
              expect(fsr['ACTION_CODE']).to eql 'REMOVE_DIRECTORY'
              if type == 'bucket'
                expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              else
                # Once there is a failure no more are performed per 4.9.5
                expect(fsr['STATUS_CODE']).to eql 'NOT_PERFORMED'
              end
              expect(fsr['FIRST_FILE_NAME']).to eql 'another_dir'
            end
          end

          it "deny file" do
            if ENV['MINIO'] && type == 'bucket'
              OpenC3::Bucket.getClient().put_object(bucket: @bucket, key: File.join(@root_path, 'deny.txt'), body: 'DENY')
            else
              File.write(File.join(SPEC_DIR, 'deny.txt'), 'DENY')
            end
            request(requests: [
              ['DENY_FILE', 'nope'],
              ['DENY_FILE', 'deny.txt'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'DENY_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'nope'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'DENY_FILE'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'deny.txt'
            end
            if ENV['MINIO'] && type == 'bucket'
              expect(OpenC3::Bucket.getClient().check_object(bucket: @bucket, key: File.join(@root_path, 'deny.txt'))).to be false
            else
              expect(File.exist?(File.join(SPEC_DIR, 'deny.txt'))).to be false
            end
          end

          it "deny directory" do
            FileUtils.mkdir(File.join(SPEC_DIR, 'deny_dir'))
            FileUtils.mkdir(File.join(SPEC_DIR, 'another_dir'))
            File.write(File.join(SPEC_DIR, 'another_dir', 'file.txt'), 'BLAH')
            request(requests: [
              ['DENY_DIRECTORY', 'nope'],
              ['DENY_DIRECTORY', 'deny_dir'],
              ['DENY_DIRECTORY', 'another_dir'],
            ]) do |indications|
              indication = indications[-1]
              expect(indication['indication_type']).to eql 'Transaction-Finished'
              expect(indication['condition_code']).to eql 'NO_ERROR'
              expect(indication['file_status']).to eql 'UNREPORTED'
              expect(indication['delivery_code']).to eql 'DATA_COMPLETE'
              expect(indication['status_report']).to eql 'FINISHED'
              fsr = indication['filestore_responses'][0]
              expect(fsr['ACTION_CODE']).to eql 'DENY_DIRECTORY'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'nope'
              fsr = indication['filestore_responses'][1]
              expect(fsr['ACTION_CODE']).to eql 'DENY_DIRECTORY'
              expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              expect(fsr['FIRST_FILE_NAME']).to eql 'deny_dir'
              fsr = indication['filestore_responses'][2]
              expect(fsr['ACTION_CODE']).to eql 'DENY_DIRECTORY'
              expect(fsr['FIRST_FILE_NAME']).to eql 'another_dir'
              if type == 'bucket'
                expect(fsr['STATUS_CODE']).to eql 'SUCCESSFUL'
              else
                expect(fsr['STATUS_CODE']).to eql 'NOT_ALLOWED'
                expect(fsr['FILESTORE_MESSAGE']).to include("not empty")
              end
            end
            FileUtils.rm_rf(File.join(SPEC_DIR, 'deny_dir')) if type == 'bucket'
            FileUtils.rm_rf(File.join(SPEC_DIR, 'another_dir'))
          end
        end
      end
    end
  end
end

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

      after(:each) do
        if @user
          @user.stop
          sleep 0.1
        end
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

      it "requires a destination_entity_id" do
        setup(source_id: 10, destination_id: 20)
        post "/cfdp/put", :params => {
          scope: "DEFAULT",
          source_file_name: 'test.txt', destination_file_name: 'test.txt'
        }
        expect(response).to have_http_status(400)
        expect(response.body).to match(/missing.*destination_entity_id/)
      end

      it "requires a source_file_name" do
        setup(source_id: 10, destination_id: 20)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          destination_file_name: 'test.txt'
        }
        expect(response).to have_http_status(400)
        expect(response.body).to match(/missing.*source_file_name/)
      end

      it "requires a destination_file_name" do
        setup(source_id: 10, destination_id: 20)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: 'test.txt'
        }
        expect(response).to have_http_status(400)
        expect(response.body).to match(/missing.*destination_file_name/)
      end

      it "requires a numeric destination_entity_id" do
        setup(source_id: 10, destination_id: 20)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: "HI",
          source_file_name: 'test.txt', destination_file_name: 'test.txt'
        }
        # TODO: This fails with 200 ... how to send 400 in the thread rescue?
        expect(response).to have_http_status(400)
      end

      it "sends a text file" do
        setup(source_id: 10, destination_id: 20)
        CfdpMib.set_entity_value(@destination_entity_id, 'maximum_file_segment_length', 8)

        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test.txt'), data)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: 'test.txt', destination_file_name: 'test.txt'
        }
        expect(response).to have_http_status(200)
        sleep 0.1
        FileUtils.rm(File.join(SPEC_DIR, 'test.txt'))

        get "/cfdp/indications", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(200)
        json = JSON.parse(response.body)
        # pp json['indications']
        expect(json['indications'].length).to eql 3
        id = json['indications'][0]['transaction_id']
        expect(id).to include(@source_entity_id.to_s)
        expect(json['indications'][0]['indication_type']).to eql 'Transaction'
        expect(json['indications'][1]['indication_type']).to eql 'EOF-Sent'
        expect(json['indications'][2]['indication_type']).to eql 'Transaction-Finished'

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

      it "creates multiple segments" do
        setup(source_id: 11, destination_id: 22)
        CfdpMib.set_entity_value(@destination_entity_id, 'maximum_file_segment_length', 8)

        data = ('a'..'z').to_a.shuffle[0,9].join
        File.write(File.join(SPEC_DIR, 'test.txt'), data)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: 'test.txt', destination_file_name: 'test.txt'
        }
        expect(response).to have_http_status(200)
        sleep 0.1
        FileUtils.rm(File.join(SPEC_DIR, 'test.txt'))

        get "/cfdp/indications", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(200)
        json = JSON.parse(response.body)
        expect(json['indications'].length).to eql 3
        id = json['indications'][0]['transaction_id']
        expect(id).to include(@source_entity_id.to_s)
        expect(json['indications'][0]['indication_type']).to eql 'Transaction'
        expect(json['indications'][1]['indication_type']).to eql 'EOF-Sent'
        expect(json['indications'][2]['indication_type']).to eql 'Transaction-Finished'

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

      it "waits for a closure" do
        setup(source_id: 10, destination_id: 20)
        CfdpMib.set_entity_value(@destination_entity_id, 'maximum_file_segment_length', 8)

        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test.txt'), data)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: 'test.txt', destination_file_name: 'test.txt',
          closure_requested: 'CLOSURE_REQUESTED'
        }
        expect(response).to have_http_status(200)
        sleep 0.1
        FileUtils.rm(File.join(SPEC_DIR, 'test.txt'))

        @user = CfdpUser.new
        @user.start
        sleep 0.1 # Allow user thread to start

        # Simlulate the finished PDU
        cmd_params = {}
        cmd_params["PDU"] = CfdpPdu.build_finished_pdu(
          source_entity: CfdpMib.entity(@source_entity_id),
          transaction_seq_num: 1,
          destination_entity: CfdpMib.entity(@destination_entity_id),
          file_size: 8,
          condition_code: "NO_ERROR",
          delivery_code: "DATA_INCOMPLETE", # Just to verify it changes
          file_status: "FILESTORE_SUCCESS")
        msg_hash = {
          :time => Time.now.to_nsec_from_epoch,
          :stored => 'false',
          :target_name => "CFDPTEST",
          :packet_name => "CFDP_PDU",
          :received_count => 1,
          :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
        }
        Topic.write_topic("DEFAULT__DECOM__{CFDPTEST}__CFDP_PDU", msg_hash, nil)
        sleep 0.1

        get "/cfdp/indications", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(200)
        json = JSON.parse(response.body)
        expect(json['indications'].length).to eql 3
        id = json['indications'][0]['transaction_id']
        expect(id).to include(@source_entity_id.to_s)
        expect(json['indications'][0]['indication_type']).to eql 'Transaction'
        expect(json['indications'][1]['indication_type']).to eql 'EOF-Sent'
        expect(json['indications'][2]['indication_type']).to eql 'Transaction-Finished'
        expect(json['indications'][2]['condition_code']).to eql 'NO_ERROR'
        expect(json['indications'][2]['delivery_code']).to eql 'DATA_INCOMPLETE'
        expect(json['indications'][2]['file_status']).to eql 'FILESTORE_SUCCESS'
      end

      it "handles timing out waiting for a closure" do
        setup(source_id: 10, destination_id: 20)
        CfdpMib.set_entity_value(@source_entity_id, 'check_limit', 0.2)

        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test.txt'), data)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: 'test.txt', destination_file_name: 'test.txt',
          closure_requested: 'CLOSURE_REQUESTED'
        }
        expect(response).to have_http_status(200)
        FileUtils.rm(File.join(SPEC_DIR, 'test.txt'))
        sleep 1.5 # Allow the timer to expire

        get "/cfdp/indications", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(200)
        json = JSON.parse(response.body)
        expect(json['indications'].length).to eql 3
        id = json['indications'][0]['transaction_id']
        expect(id).to include(@source_entity_id.to_s)
        expect(json['indications'][0]['indication_type']).to eql 'Transaction'
        expect(json['indications'][1]['indication_type']).to eql 'EOF-Sent'
        expect(json['indications'][2]['indication_type']).to eql 'Transaction-Finished'
        expect(json['indications'][2]['condition_code']).to eql 'CHECK_LIMIT_REACHED'
        expect(json['indications'][2]['delivery_code']).to eql 'DATA_COMPLETE'
        expect(json['indications'][2]['file_status']).to eql 'UNREPORTED'
      end

      it "handles bad transaction IDs" do
        setup(source_id: 1, destination_id: 2)

        @user = CfdpUser.new
        thread = @user.start
        sleep 0.1 # Allow user thread to start
        expect(thread.alive?).to be true

        cmd_params = {}
        cmd_params["PDU"] = CfdpPdu.build_file_data_pdu(
          source_entity: CfdpMib.entity(@source_entity_id),
          transaction_seq_num: 1,
          destination_entity: CfdpMib.entity(@destination_entity_id),
          file_size: 8,
          offset: 0,
          file_data: "\x00")
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
      end

      it "receives data" do
        setup(source_id: 12, destination_id: 33)
        CfdpMib.set_entity_value(@destination_entity_id, 'maximum_file_segment_length', 8)

        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test.txt'), data)
        post "/cfdp/put", :params => {
          scope: "DEFAULT", destination_entity_id: @destination_entity_id,
          source_file_name: 'test.txt', destination_file_name: 'test.txt'
        }
        expect(response).to have_http_status(200)
        sleep 0.1
        FileUtils.rm(File.join(SPEC_DIR, 'test.txt'))

        # Clear the tx transactions to simulate the receive side on the same system
        keys = CfdpMib.transactions.keys
        keys.each do |key|
          CfdpMib.transactions.delete(key)
        end

        @user = CfdpUser.new
        @user.start
        sleep 0.1 # Allow user thread to start

        @packets.each do |target_name, cmd_name, cmd_params|
          msg_hash = {
            :time => Time.now.to_nsec_from_epoch,
            :stored => 'false',
            :target_name => target_name,
            :packet_name => cmd_name,
            :received_count => 1,
            :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
          }
          Topic.write_topic("DEFAULT__DECOM__{#{target_name}}__#{cmd_name}", msg_hash, nil)
        end
        sleep 0.1

        expect(@rx_pdus.length).to eql 1
        expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        # We're the source_entity_id as well as the destination in this case
        expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @source_entity_id
        expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'FINISHED'
        expect(@rx_pdus[0]['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
        expect(@rx_pdus[0]['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'
      end
    end
  end
end
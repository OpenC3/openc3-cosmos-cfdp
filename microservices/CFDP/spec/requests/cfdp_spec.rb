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
        CfdpMib.clear
        @root_path = SPEC_DIR
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt') if File.exist?(File.join(SPEC_DIR, 'test1.txt'))
        FileUtils.rm File.join(SPEC_DIR, 'test2.txt') if File.exist?(File.join(SPEC_DIR, 'test2.txt'))
        FileUtils.rm File.join(SPEC_DIR, 'new_file.txt') if File.exist?(File.join(SPEC_DIR, 'new_file.txt'))
        FileUtils.rm File.join(SPEC_DIR, 'rename_file.txt') if File.exist?(File.join(SPEC_DIR, 'rename_file.txt'))
        FileUtils.rm File.join(SPEC_DIR, 'deny.txt') if File.exist?(File.join(SPEC_DIR, 'deny.txt'))
        FileUtils.rm File.join(SPEC_DIR, 'first.txt') if File.exist?(File.join(SPEC_DIR, 'first.txt'))
        FileUtils.rm File.join(SPEC_DIR, 'second.txt') if File.exist?(File.join(SPEC_DIR, 'second.txt'))
        FileUtils.rm_rf(File.join(SPEC_DIR, 'new_dir')) if File.exist?(File.join(SPEC_DIR, 'new_dir'))
        FileUtils.rm_rf(File.join(SPEC_DIR, 'deny_dir')) if File.exist?(File.join(SPEC_DIR, 'deny_dir'))
        FileUtils.rm_rf(File.join(SPEC_DIR, 'another_dir')) if File.exist?(File.join(SPEC_DIR, 'another_dir'))
      end

      def safe_write_topic(*args)
        begin
          Topic.write_topic(*args)
        rescue RuntimeError
          # Work around mock_redis hash issue
          sleep(0.1)
          retry
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
        @source_packets = []
        @receive_packets = []
        @source_packet_mutex = Mutex.new
        @receive_packet_mutex = Mutex.new
        allow_any_instance_of(CfdpSourceTransaction).to receive('cmd') do |source, tgt_name, pkt_name, params|
          # puts params["PDU"].formatted # Prints the raw bytes
          @source_packet_mutex.synchronize do
            @source_packets << [tgt_name, pkt_name, params]
            begin
              @tx_pdus << CfdpPdu.decom(params["PDU"])
            rescue
            end
          end
        end
        allow_any_instance_of(CfdpReceiveTransaction).to receive('cmd') do |source, tgt_name, pkt_name, params|
          @receive_packet_mutex.synchronize do
            @receive_packets << [tgt_name, pkt_name, params]
            begin
              @rx_pdus << CfdpPdu.decom(params["PDU"])
            rescue
            end
          end
        end
      end

      # Helper method to perform a filestore_request and return the indication
      # This does all the work and creates all the fault conditions using keyword args
      def request(source: nil, dest: nil, requests: [], overrides: [], messages: [], flow_label: nil,
                  transmission_mode: 'UNACKNOWLEDGED', closure: 'CLOSURE_NOT_REQUESTED',
                  send_closure: true, cancel: false, skip: false, duplicate_metadata: false,
                  duplicate_filedata: false, bad_seg_size: false, eof_size: nil,
                  prompt: nil, crcs_required: nil, cycles: 1)
        setup(source_id: 1, destination_id: 2) unless CfdpMib.entity(@destination_entity_id)
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
          transmission_mode: transmission_mode,
          fault_handler_overrides: overrides,
          messages_to_user: messages,
          flow_label: flow_label
        }, as: :json
        expect(response).to have_http_status(200)
        sleep 0.1

        # Start user thread here so it has the chance to receive the closure PDU
        @user = CfdpUser.new
        @user.start
        sleep 0.5 # Allow user thread to start

        rx_transactions = {}
        tx_transactions = {}
        source_packet_index = 0
        receive_packet_index = 0
        cycles.times do
          # Clear the tx transactions to simulate the receive side on the same system
          tx_transactions = CfdpMib.transactions.clone
          keys = CfdpMib.transactions.keys
          keys.each do |key|
            CfdpMib.transactions.delete(key)
          end

          # Restore the rx transactions and send the receive_packets
          rx_transactions.each do |key, value|
            # puts "restore:#{key} val:#{value}"
            CfdpMib.transactions[key] = value
          end

          CfdpMib.source_entity_id = @destination_entity_id
          CfdpMib.set_entity_value(@destination_entity_id, 'crcs_required', true) if crcs_required

          i = -1
          @source_packet_mutex.synchronize do
            @source_packets[source_packet_index..-1].each do |target_name, cmd_name, cmd_params|
              i += 1
              # Skip is an array of segments to skip
              # 1 based since segments start at packet 1, metadata is 0
              next if skip and skip.include?(i)

              if eof_size and i == 2
                # See the cfdp_pdu_eof_spec.rb for the structure
                cmd_params['PDU'][16] = [eof_size].pack('C') #"\x07" # Hack to be less than 8
              end
              if bad_seg_size and i == 2
                # See the cfdp_pdu_file_data_spec.rb for the structure
                cmd_params['PDU'][10] = "\x0A" # Hack to be more than 8
              end

              if prompt
                # Need to sneak the prompt before the EOF PDU which is last
                if i == @source_packets.length - 1
                  # Simlulate the prompt PDU
                  prompt_params = {}
                  prompt_params['PDU'] = CfdpPdu.build_prompt_pdu(
                    source_entity: CfdpMib.entity(@source_entity_id),
                    transaction_seq_num: 1,
                    destination_entity: CfdpMib.entity(@destination_entity_id),
                    transmission_mode: transmission_mode,
                    response_required: prompt
                  )
                  msg_hash = {
                    :time => Time.now.to_nsec_from_epoch,
                    :stored => 'false',
                    :target_name => "CFDPTEST",
                    :packet_name => "CFDP_PDU",
                    :received_count => 1,
                    :json_data => JSON.generate(prompt_params.as_json(:allow_nan => true)),
                  }
                  safe_write_topic("DEFAULT__DECOM__{CFDPTEST}__CFDP_PDU", msg_hash, nil)
                end
              end

              msg_hash = {
                :time => Time.now.to_nsec_from_epoch,
                :stored => 'false',
                :target_name => target_name,
                :packet_name => cmd_name,
                :received_count => 1,
                :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
              }
              safe_write_topic("DEFAULT__DECOM__{#{target_name}}__#{cmd_name}", msg_hash, nil)
              # Duplicate metadata should be ignored
              if i == 0 and duplicate_metadata
                safe_write_topic("DEFAULT__DECOM__{#{target_name}}__#{cmd_name}", msg_hash, nil)
              end
              if i == 1
                if duplicate_filedata
                  safe_write_topic("DEFAULT__DECOM__{#{target_name}}__#{cmd_name}", msg_hash, nil)
                end
                if cancel
                  post "/cfdp/cancel", :params => {
                    scope: "DEFAULT", transaction_id: "#{@source_entity_id}__1"
                  }, as: :json
                  sleep 0.5
                end
              end
            end
            source_packet_index = @source_packets.length
          end
          sleep 0.5 # Allow them to be processed

          # Clear the rx transactions to simulate the transmit side on the same system
          rx_transactions = CfdpMib.transactions.clone
          keys = CfdpMib.transactions.keys
          keys.each do |key|
            CfdpMib.transactions.delete(key)
          end

          # Restore the tx transactions and send the receive_packets
          tx_transactions.each do |key, value|
            # puts "restore:#{key} val:#{value}"
            CfdpMib.transactions[key] = value
          end
          CfdpMib.source_entity_id = @source_entity_id

          @receive_packet_mutex.synchronize do
            @receive_packets[receive_packet_index..-1].each do |target_name, cmd_name, cmd_params|
              if send_closure
                msg_hash = {
                  :time => Time.now.to_nsec_from_epoch,
                  :stored => 'false',
                  :target_name => target_name,
                  :packet_name => cmd_name,
                  :received_count => 1,
                  :json_data => JSON.generate(cmd_params.as_json(:allow_nan => true)),
                }
                safe_write_topic("DEFAULT__DECOM__{#{target_name}}__#{cmd_name}", msg_hash, nil)
              end
            end
            receive_packet_index = @receive_packets.length
          end

          sleep 0.5
        end

        @user.stop
        sleep 0.1

        get "/cfdp/indications", :params => { scope: "DEFAULT" }
        expect(response).to have_http_status(200)
        json = JSON.parse(response.body)
        yield json['indications'] if block_given?
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

      # Section 4.1 CRC Procedures
      [true, false].each do |required|
        context "with CRCs required #{required}" do
          it "sends a file" do
            setup(source_id: 1, destination_id: 2)
            CfdpMib.set_entity_value(@source_entity_id, 'crcs_required', required)
            CfdpMib.set_entity_value(@destination_entity_id, 'crcs_required', required)

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
              expect(indications[3]['file_size']).to eql 8
              expect(indications[3]['source_entity_id']).to eql 1
              expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
              expect(indications[4]['offset']).to eql 0
              expect(indications[4]['length']).to eql 8
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

            expect(CfdpMib.entity(@destination_entity_id)['crcs_required']).to be required
            # REQ 4.1.3.1, 4.1.3.2
            # Check the CRC algorithm and position
            @source_packets.each do |packet|
              pdu_data = packet[2]["PDU"]
              calculated = OpenC3::Crc16.new.calc(pdu_data[0..-3])
              if required
                expect(calculated).to eql pdu_data[-2..-1].unpack("n")[0]
              else
                expect(calculated).not_to eql pdu_data[-2..-1].unpack("n")[0]
              end
            end

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
        end
      end

      it "raises if CRCs are required and do not exist" do
        setup(source_id: 1, destination_id: 2)
        CfdpMib.set_entity_value(@source_entity_id, 'crcs_required', true)
        CfdpMib.set_entity_value(@destination_entity_id, 'crcs_required', false)
        # We get three exceptions from the 3 PDUs that we process
        expect(Logger).to receive(:error).exactly(3).times.with(/PDU without required CRC received/, scope: 'DEFAULT')
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt', crcs_required: true)
        expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
      end
      # END Section 4.1 CRC Procedures

      # Section 4.2 Checksum Procedures
      it "uses a null checksum (zeros)" do
        setup(source_id: 1, destination_id: 2)
        CfdpMib.set_entity_value(@source_entity_id, 'default_checksum_type', 15)
        CfdpMib.set_entity_value(@destination_entity_id, 'default_checksum_type', 15)
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
          expect(indications[3]['file_size']).to eql 8
          expect(indications[3]['source_entity_id']).to eql 1
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
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
        # NULL Checksum is 0
        expect(@tx_pdus[2]['FILE_CHECKSUM']).to eql 0
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
          expect(indications[3]['file_size']).to eql 8
          expect(indications[3]['source_entity_id']).to eql 1
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
          expect(indications[5]['indication_type']).to eql 'EOF-Recv'
          # 4.6.1.2.8 d. File Checksum Failure fault
          expect(indications[6]['indication_type']).to eql 'Fault'
          expect(indications[6]['condition_code']).to eql 'FILE_CHECKSUM_FAILURE'
          expect(indications[6]['progress']).to eql 8
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
        expect(@tx_pdus[2]['FILE_CHECKSUM']).not_to eql 0
      end
      # END Section 4.2 Checksum Procedures

      # Section 4.3 Put Procedures
      it "requests metadata only with no source and dest" do
        request() do |indications|
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[1]['indication_type']).to eql 'EOF-Sent'
          expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[2]['condition_code']).to eql 'NO_ERROR'
          expect(indications[2]['file_status']).to eql 'UNREPORTED'
          expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[2]['status_report']).to eql 'FINISHED'
          # Receive indications
          expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[3]['source_file_name']).to be nil
          expect(indications[3]['destination_file_name']).to be nil
          expect(indications[3]['file_size']).to eql 0
          expect(indications[3]['source_entity_id']).to eql 1
          expect(indications[4]['indication_type']).to eql 'EOF-Recv'
          expect(indications[5]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[5]['condition_code']).to eql 'NO_ERROR'
          expect(indications[5]['file_status']).to eql 'UNREPORTED'
          expect(indications[5]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[5]['status_report']).to eql 'FINISHED'
        end
        # Validate the PDUs
        expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
        expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[1]['DIRECTIVE_CODE']).to eql 'EOF'
        expect(@tx_pdus[2]).to be nil
      end
      # END Section 4.3 Put Procedures

      # Section 4.4 Transaction Start Notification Procedure
      it "creates unique transactions IDs" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt')
        expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test3.txt') do |indications|
          # The existing notifications are still there
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[0]['transaction_id']).to eql '1__1'
          # New transaction has new ID
          expect(indications[7]['indication_type']).to eql 'Transaction'
          expect(indications[7]['transaction_id']).to eql '1__2'
        end
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
        FileUtils.rm File.join(SPEC_DIR, 'test2.txt')
        FileUtils.rm File.join(SPEC_DIR, 'test3.txt')

        # Validate the PDUs
        expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[0]['SEQUENCE_NUMBER']).to eql 1
        expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
        expect(@tx_pdus[1]['SEQUENCE_NUMBER']).to eql 1
        expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[2]['SEQUENCE_NUMBER']).to eql 1

        expect(@tx_pdus[3]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[3]['SEQUENCE_NUMBER']).to eql 2
        expect(@tx_pdus[4]['TYPE']).to eql 'FILE_DATA'
        expect(@tx_pdus[4]['SEQUENCE_NUMBER']).to eql 2
        expect(@tx_pdus[5]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[5]['SEQUENCE_NUMBER']).to eql 2
      end
      # END Section 4.4 Transaction Start Notification Procedure

      # No tests for Section 4.5 PDU FORWARDING PROCEDURES

      # Section 4.6 COPY FILE PROCEDURES

      # 4.6.1.1.1 Sending Entity Unacknowleged
      # 4.6.1.2.1 Receving Entity Unacknowleged
      it "performs unacknowldged transfers" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                transmission_mode: 'UNACKNOWLEDGED',
                messages: [
                  'This is a test',
                  'Another message'
                ],
                overrides: [
                  ['ACK_LIMIT_REACHED', 'ISSUE_NOTICE_OF_CANCELLATION'],
                  ['FILE_CHECKSUM_FAILURE', 'IGNORE_ERROR']
                ],
                requests: [
                  ['CREATE_FILE', "temp.txt"],
                  ['DELETE_FILE', "temp.txt"],
                ],
                flow_label: 'FLOW') do |indications|
          # First the transmit indications
          expect(indications[0]['indication_type']).to eql 'Transaction'
          # 4.6.1.1.9 b. EOF-Sent indication
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
          expect(indications[3]['file_size']).to eql 8
          expect(indications[3]['source_entity_id']).to eql 1
          expect(indications[3]['fault_handler_overrides']).to eql({"ACK_LIMIT_REACHED"=>"ISSUE_NOTICE_OF_CANCELLATION", "FILE_CHECKSUM_FAILURE"=>"IGNORE_ERROR"})
          # 4.6.1.2.6 Metadata-Recv includes messags to user
          expect(indications[3]['messages_to_user']).to eql ["This is a test", "Another message"]
          expect(indications[3]['filestore_requests']).to eql [{"ACTION_CODE"=>"CREATE_FILE", "FIRST_FILE_NAME"=>"temp.txt"}, {"ACTION_CODE"=>"DELETE_FILE", "FIRST_FILE_NAME"=>"temp.txt"}]
          expect(indications[3]['flow_label']).to eql "FLOW"
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
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

        # 4.6.1.1.2 Metadata PDU
        expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
        # 4.6.1.1.3 a. Metadata PDU contents size of the file
        expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length
        # 4.6.1.1.3 b. Metadata PDU contents source name
        expect(@tx_pdus[0]['SOURCE_FILE_NAME']).to eql 'test1.txt'
        # 4.6.1.1.3 b. Metadata PDU contents destination name
        expect(@tx_pdus[0]['DESTINATION_FILE_NAME']).to eql 'test2.txt'
        # 4.6.1.1.3 c. Metadata PDU contents Fault handler overrides
        expect(@tx_pdus[0]['TLVS'][0]['TYPE']).to eql 'FAULT_HANDLER_OVERRIDE'
        expect(@tx_pdus[0]['TLVS'][0]['CONDITION_CODE']).to eql 'ACK_LIMIT_REACHED'
        expect(@tx_pdus[0]['TLVS'][0]['HANDLER_CODE']).to eql 'ISSUE_NOTICE_OF_CANCELLATION'
        expect(@tx_pdus[0]['TLVS'][1]['TYPE']).to eql 'FAULT_HANDLER_OVERRIDE'
        expect(@tx_pdus[0]['TLVS'][1]['CONDITION_CODE']).to eql 'FILE_CHECKSUM_FAILURE'
        expect(@tx_pdus[0]['TLVS'][1]['HANDLER_CODE']).to eql 'IGNORE_ERROR'
        # 4.6.1.1.3 c. Metadata PDU contents Messages to User
        expect(@tx_pdus[0]['TLVS'][2]['TYPE']).to eql 'MESSAGE_TO_USER'
        expect(@tx_pdus[0]['TLVS'][2]['MESSAGE_TO_USER']).to eql 'This is a test'
        expect(@tx_pdus[0]['TLVS'][3]['TYPE']).to eql 'MESSAGE_TO_USER'
        expect(@tx_pdus[0]['TLVS'][3]['MESSAGE_TO_USER']).to eql 'Another message'
        # 4.6.1.1.3 c. Metadata PDU contents Filestore requests
        expect(@tx_pdus[0]['TLVS'][4]['TYPE']).to eql 'FILESTORE_REQUEST'
        expect(@tx_pdus[0]['TLVS'][4]['ACTION_CODE']).to eql 'CREATE_FILE'
        expect(@tx_pdus[0]['TLVS'][5]['TYPE']).to eql 'FILESTORE_REQUEST'
        expect(@tx_pdus[0]['TLVS'][5]['ACTION_CODE']).to eql 'DELETE_FILE'
        # 4.6.1.1.3 c. Metadata PDU contents Flow label
        expect(@tx_pdus[0]['TLVS'][6]['TYPE']).to eql 'FLOW_LABEL'
        expect(@tx_pdus[0]['TLVS'][6]['FLOW_LABEL']).to eql 'FLOW'
        # 4.6.1.1.3 d. Metadata PDU contents
        expect(@tx_pdus[0]['CLOSURE_REQUESTED']).to eql 'CLOSURE_NOT_REQUESTED'
        # 4.6.1.1.3 e. Metadata PDU contents
        expect(@tx_pdus[0]['CHECKSUM_TYPE']).to eql 0
        # 4.6.1.1.1 Unacknowledged
        expect(@tx_pdus[0]['TRANSMISSION_MODE']).to eql 'UNACKNOWLEDGED'

        # 4.6.1.1.4 File Data PDUs
        expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
        expect(@tx_pdus[1]['FILE_DATA']).to eql data
        # 4.6.1.1.5 Offset of segment
        expect(@tx_pdus[1]['OFFSET']).to eql 0
        # 4.6.1.1.5.1 Segment Metadata
        expect(@tx_pdus[1]['SEGMENT_METADATA_FLAG']).to eql 'NOT_PRESENT'
        # 4.6.1.1.5.[2,3,4] All relate to segment metadata ... not implemented
        # 4.6.1.1.6, 4.6.1.1.7 Segmentation control
        expect(@tx_pdus[1]['SEGMENTATION_CONTROL']).to eql 'NOT_PRESERVED'
        # 4.6.1.1.8 Segmentation control invalid ... not implemented

        expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        # 4.6.1.1.9 a. EOF PDU
        expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
        # 4.6.1.1.9 c. EOF PDU checksum & length
        expect(@tx_pdus[2]['FILE_CHECKSUM']).not_to be nil
        expect(@tx_pdus[2]['FILE_SIZE']).to eql 8
        # 4.6.1.1.10 Flow label is implementation specific ... not implemented
      end

      # 4.6.1.1.1 Sending Entity Acknowleged
      # 4.6.1.2.1 Receving Entity Acknowleged
      it "sends data acknowledged" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                transmission_mode: 'ACKNOWLEDGED',
                # 4.6.1.2.4 Duplicate metadata discarded
                duplicate_metadata: true) do |indications|
          # First the transmit indications
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[1]['indication_type']).to eql 'EOF-Sent'
          # Receive indications
          # 4.6.1.2.6 Metadata-Recv indication
          expect(indications[2]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[2]['source_file_name']).to eql 'test1.txt'
          expect(indications[2]['destination_file_name']).to eql 'test2.txt'
          expect(indications[2]['file_size']).to eql 8
          expect(indications[2]['source_entity_id']).to eql 1
          expect(indications[3]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[3]['offset']).to eql 0
          expect(indications[3]['length']).to eql 8
          expect(indications[4]['indication_type']).to eql 'EOF-Recv'
          expect(indications[5]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[5]['condition_code']).to eql 'NO_ERROR'
          expect(indications[5]['file_status']).to eql 'FILESTORE_SUCCESS'
          expect(indications[5]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[5]['status_report']).to eql 'FINISHED'

          # Final transmit
          # 4.6.3.2.1 Transmission of EOF causes Notice of Completion
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
        # 4.6.1.1.1 Acknowledged
        expect(@tx_pdus[0]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

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
        # 4.6.4.2.4 ACK Finished
        expect(@tx_pdus[3]['DIRECTIVE_CODE']).to eql 'ACK'
        expect(@tx_pdus[3]['ACK_DIRECTIVE_CODE']).to eql 'FINISHED'
        expect(@tx_pdus[3]['ACK_DIRECTIVE_SUBTYPE']).to eql 1
        expect(@tx_pdus[3]['CONDITION_CODE']).to eql 'NO_ERROR'
        expect(@tx_pdus[3]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        expect(@tx_pdus[3]['TRANSACTION_STATUS']).to eql 'ACTIVE' # TODO: ACTIVE?

        # Validate the Rx PDUs
        expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@rx_pdus[0]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        # 4.6.4.3.5 ACK the EOF
        expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'ACK'
        expect(@rx_pdus[0]['ACK_DIRECTIVE_CODE']).to eql 'EOF'
        expect(@rx_pdus[0]['ACK_DIRECTIVE_SUBTYPE']).to eql 0
        expect(@rx_pdus[0]['CONDITION_CODE']).to eql 'NO_ERROR'

        expect(@rx_pdus[1]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@rx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@rx_pdus[1]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        # 4.6.4.3.4 Finished PDU when complete
        expect(@rx_pdus[1]['DIRECTIVE_CODE']).to eql 'FINISHED'
        expect(@rx_pdus[1]['CONDITION_CODE']).to eql 'NO_ERROR'
        expect(@rx_pdus[1]['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
        expect(@rx_pdus[1]['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'

        expect(@rx_pdus[2]).to be nil
      end

      it "detects a single missing data PDU" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                transmission_mode: 'ACKNOWLEDGED',
                skip: [1], cycles: 2) do |indications|
          # First the transmit indications
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[1]['indication_type']).to eql 'EOF-Sent'
          # Receive indications
          # 4.6.1.2.6 Metadata-Recv indication
          expect(indications[2]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[2]['source_file_name']).to eql 'test1.txt'
          expect(indications[2]['destination_file_name']).to eql 'test2.txt'
          expect(indications[2]['file_size']).to eql 8
          expect(indications[2]['source_entity_id']).to eql 1
          expect(indications[3]['indication_type']).to eql 'EOF-Recv'
        end
        expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true

        # Validate the Tx PDUs
        expect(@tx_pdus.length).to eql 5
        expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@tx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
        # 4.6.1.1.1 Acknowledged
        expect(@tx_pdus[0]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        expect(@tx_pdus[0]['FILE_SIZE']).to eql data.length

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

        expect(@tx_pdus[3]['TYPE']).to eql 'FILE_DATA'
        expect(@tx_pdus[3]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@tx_pdus[3]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@tx_pdus[3]['FILE_DATA']).to eql data
        expect(@tx_pdus[3]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'

        expect(@tx_pdus[4]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[4]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@tx_pdus[4]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@tx_pdus[4]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        expect(@tx_pdus[4]['DIRECTIVE_CODE']).to eql 'ACK'
        expect(@tx_pdus[4]['ACK_DIRECTIVE_CODE']).to eql 'FINISHED'

        # Validate the Rx PDUs
        expect(@rx_pdus.length).to eql 3
        expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@rx_pdus[0]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        # 4.6.4.3.5 ACK the EOF
        expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'ACK'
        expect(@rx_pdus[0]['ACK_DIRECTIVE_CODE']).to eql 'EOF'
        expect(@rx_pdus[0]['ACK_DIRECTIVE_SUBTYPE']).to eql 0
        expect(@rx_pdus[0]['CONDITION_CODE']).to eql 'NO_ERROR'

        expect(@rx_pdus[1]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@rx_pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@rx_pdus[1]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        expect(@rx_pdus[1]['DIRECTIVE_CODE']).to eql 'NAK'
        expect(@rx_pdus[1]['START_OF_SCOPE']).to eql 0
        expect(@rx_pdus[1]['END_OF_SCOPE']).to eql 8
        expect(@rx_pdus[1]['SEGMENT_REQUESTS'].length).to eql 1
        expect(@rx_pdus[1]['SEGMENT_REQUESTS'][0]["START_OFFSET"]).to eql 0
        expect(@rx_pdus[1]['SEGMENT_REQUESTS'][0]["END_OFFSET"]).to eql 8

        expect(@rx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@rx_pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@rx_pdus[2]['TRANSMISSION_MODE']).to eql 'ACKNOWLEDGED'
        # 4.6.4.3.4 Finished PDU when complete
        expect(@rx_pdus[2]['DIRECTIVE_CODE']).to eql 'FINISHED'
        expect(@rx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
        expect(@rx_pdus[2]['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
        expect(@rx_pdus[2]['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'
      end

      # 4.6.1.2.7 a. Repeated data is discarded
      it "handles repeated data" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                duplicate_filedata: true) do |indications|
          # Transmit indications
          expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[2]['condition_code']).to eql 'NO_ERROR'
          expect(indications[2]['file_status']).to eql 'UNREPORTED'
          expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[2]['status_report']).to eql 'FINISHED'
          # Receive indications
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
        expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
        expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
        expect(@tx_pdus[1]['FILE_DATA']).to eql data
        expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
        expect(@tx_pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
        expect(@rx_pdus[0]).to be nil
      end

      # 4.6.1.2.7 b. Segementation control flag ... not implemented

      # 4.6.1.2.7 c. Sum of offset and segment exceeds file size
      it "reports file size error if sum of offset and segment exceeds file size" do
        setup(source_id: 1, destination_id: 2)
        # Disable CRCs so the hacked PDU will be processed
        CfdpMib.set_entity_value(@source_entity_id, 'crcs_required', false)
        CfdpMib.set_entity_value(@destination_entity_id, 'crcs_required', false)

        data = ('a'..'z').to_a.shuffle[0,9].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                bad_seg_size: true) do |indications|
          # Transmit indications
          expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[2]['condition_code']).to eql 'NO_ERROR'
          expect(indications[2]['file_status']).to eql 'UNREPORTED'
          expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[2]['status_report']).to eql 'FINISHED'
          # Receive indications
          expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[3]['file_size']).to eql 9
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
          # 4.6.1.2.7 c. Sum of offset and segment exceeds File Size Error fault
          expect(indications[5]['indication_type']).to eql 'Fault'
          expect(indications[5]['condition_code']).to eql 'FILE_SIZE_ERROR'
          # 11 which is hacked length of 10 plus file size of 1
          expect(indications[5]['progress']).to eql 11
          expect(indications[6]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[6]['offset']).to eql 10
          expect(indications[6]['length']).to eql 1
          # 4.6.1.2.9 EOF generates File Size Error fault
          expect(indications[7]['indication_type']).to eql 'Fault'
          expect(indications[7]['condition_code']).to eql 'FILE_SIZE_ERROR'
          expect(indications[7]['progress']).to eql 11
          expect(indications[8]['indication_type']).to eql 'EOF-Recv'
        end
        expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
      end

      # 4.6.1.2.9 progress exceeds file size
      it "reports file size error if progress exceeds file size" do
        setup(source_id: 1, destination_id: 2)
        # Disable CRCs so the hacked PDU will be processed
        CfdpMib.set_entity_value(@source_entity_id, 'crcs_required', false)
        CfdpMib.set_entity_value(@destination_entity_id, 'crcs_required', false)

        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                eof_size: 7) do |indications|
          # Transmit indications
          expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[2]['condition_code']).to eql 'NO_ERROR'
          expect(indications[2]['file_status']).to eql 'UNREPORTED'
          expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[2]['status_report']).to eql 'FINISHED'
          # Receive indications
          expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[3]['file_size']).to eql 8
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
          # 4.6.1.2.9 EOF generates File Size Error fault
          expect(indications[5]['indication_type']).to eql 'Fault'
          expect(indications[5]['condition_code']).to eql 'FILE_SIZE_ERROR'
          expect(indications[5]['progress']).to eql 8
          expect(indications[6]['indication_type']).to eql 'EOF-Recv'
        end
        expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true

        # Validate the RX PDU
        expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'NAK'
        expect(@rx_pdus[0]['START_OF_SCOPE']).to eql 0
        expect(@rx_pdus[0]['END_OF_SCOPE']).to eql 7 # Our bad eof_size
        expect(@rx_pdus[0]['SEGMENT_REQUESTS']).to eql [] # TODO: Nothing?
      end

      # 4.6.1.2.10 Flow label optional & implementation specific ... not implemented

      # # 4.6.4.2.1 Respond to NAKs by retransmitting
      # it "responds to NAKs by retransmitting" do
      #   data = ('a'..'z').to_a.shuffle[0,8].join
      #   File.write(File.join(SPEC_DIR, 'test1.txt'), data)
      #   request(source: 'test1.txt', dest: 'test2.txt',
      #           transmission_mode: 'ACKNOWLEDGED',
      #           skip: [1],
      #           prompt: 'NAK') do |indications|
      #             pp indications
      #     # Transmit indications
      #     # expect(indications[2]['indication_type']).to eql 'Transaction-Finished'
      #     # expect(indications[2]['condition_code']).to eql 'NO_ERROR'
      #     # expect(indications[2]['file_status']).to eql 'UNREPORTED'
      #     # expect(indications[2]['delivery_code']).to eql 'DATA_COMPLETE'
      #     # expect(indications[2]['status_report']).to eql 'FINISHED'
      #     # # Receive indications
      #     # expect(indications[3]['indication_type']).to eql 'Metadata-Recv'
      #     # expect(indications[3]['file_size']).to eql '8'
      #     # expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
      #     # expect(indications[4]['offset']).to eql '0'
      #     # expect(indications[4]['length']).to eql '8'
      #     # # 4.6.1.2.9 EOF generates File Size Error fault
      #     # expect(indications[5]['indication_type']).to eql 'Fault'
      #     # expect(indications[5]['condition_code']).to eql 'FILE_SIZE_ERROR'
      #     # expect(indications[5]['progress']).to eql '8'
      #     # expect(indications[6]['indication_type']).to eql 'EOF-Recv'
      #   end
      #   # expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
      #   # FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
      #   pp @tx_pdus
      #   pp @rx_pdus
      # end

      # 4.6.4.3.2 Lost Metadata
      # TODO: Send a transfer but not the metadata and check the NAK for 0-0

      # Test various segments missing
      # 4.6.4.3.1 File data gap
      %w(1 2 3 1_2 2_3 1_3).each do |missing|
        context "with segment #{missing} missing" do
          it "sends a NAK" do
            setup(source_id: 1, destination_id: 2)
            # Ignore all the errors about no such file "test1.txt"
            # that happen because of the NAK
            allow(OpenC3::Logger).to receive(:error)
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
              expect(indications[3]['file_size']).to eql 17
              expect(indications[3]['source_entity_id']).to eql 1
              # Segment indications
              i = 4
              unless segments.include?(1)
                expect(indications[i]['indication_type']).to eql 'File-Segment-Recv'
                expect(indications[i]['offset']).to eql 0
                expect(indications[i]['length']).to eql 8
                i += 1
              end
              unless segments.include?(2)
                expect(indications[i]['indication_type']).to eql 'File-Segment-Recv'
                expect(indications[i]['offset']).to eql 8
                expect(indications[i]['length']).to eql 8
                i += 1
              end
              unless segments.include?(3)
                expect(indications[i]['indication_type']).to eql 'File-Segment-Recv'
                expect(indications[i]['offset']).to eql 16
                expect(indications[i]['length']).to eql 1
                i += 1
              end
              expect(indications[i]['indication_type']).to eql 'EOF-Recv'
            end
            expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be false
            FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true

            # Validate the TX PDUs
            expect(@tx_pdus.length).to eql (5 + segments.length)
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

            index = 0
            segments.each do |segment|
              expect(@tx_pdus[5 + index]['TYPE']).to eql 'FILE_DATA'
              expect(@tx_pdus[5 + index]['SOURCE_ENTITY_ID']).to eql @source_entity_id
              expect(@tx_pdus[5 + index]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
              if segment == 1
                expect(@tx_pdus[5 + index]['FILE_DATA']).to eql data[0..7]
              elsif segment == 2
                expect(@tx_pdus[5 + index]['FILE_DATA']).to eql data[8..15]
              else
                expect(@tx_pdus[5 + index]['FILE_DATA']).to eql data[16..-1]
              end
              index += 1
            end

            # Validate the RX PDUs
            expect(@rx_pdus.length).to eql 1
            expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
            expect(@rx_pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
            expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
            # 4.6.4.4.2 Immediate NAK mode
            expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'NAK'
            # 4.6.4.3.3 a Scope of the NAK is start and end offset
            expect(@rx_pdus[0]['START_OF_SCOPE']).to eql 0
            expect(@rx_pdus[0]['END_OF_SCOPE']).to eql 17
            # 4.6.4.3.3 b Segment requests should indicate only the segments not yet received
            if segments == [1,3] # Special case
              expect(@rx_pdus[0]['SEGMENT_REQUESTS']).to eql [{"START_OFFSET"=>0, "END_OFFSET"=>8}, {"START_OFFSET"=>16, "END_OFFSET"=>17}]
            else
              expect(@rx_pdus[0]['SEGMENT_REQUESTS']).to eql [{"START_OFFSET"=>start_offset, "END_OFFSET"=>end_offset}]
            end
          end
        end
      end

      # 4.6.5.2.2 Send Keep Alive as Prompt response
      it "sends keep alive response to prompt" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt',
                transmission_mode: 'ACKNOWLEDGED',
                prompt: 'KEEP_ALIVE') do |indications|
          # First the transmit indications
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[1]['indication_type']).to eql 'EOF-Sent'
          # Receive indications
          expect(indications[2]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[3]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['indication_type']).to eql 'EOF-Recv'
          expect(indications[5]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[5]['condition_code']).to eql 'NO_ERROR'
          expect(indications[5]['file_status']).to eql 'FILESTORE_SUCCESS'
          expect(indications[5]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[5]['status_report']).to eql 'FINISHED'
          # Final transmit
          expect(indications[6]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[6]['condition_code']).to eql 'NO_ERROR'
          expect(indications[6]['file_status']).to eql 'FILESTORE_SUCCESS'
          expect(indications[6]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[6]['status_report']).to eql 'FINISHED'
        end
        expect(File.exist?(File.join(SPEC_DIR, 'test2.txt'))).to be true
        FileUtils.rm File.join(SPEC_DIR, 'test1.txt'), force: true
        FileUtils.rm File.join(SPEC_DIR, 'test2.txt')

        # Validate the TX PDUs
        expect(@tx_pdus.length).to eql 4
        expect(@tx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
        expect(@tx_pdus[1]['TYPE']).to eql 'FILE_DATA'
        expect(@tx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
        expect(@tx_pdus[3]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@tx_pdus[3]['DIRECTIVE_CODE']).to eql 'ACK'

        # Validate the RX PDUs
        expect(@rx_pdus.length).to eql 3
        expect(@rx_pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'KEEP_ALIVE'
        expect(@rx_pdus[0]['PROGRESS']).to eql 8
        expect(@rx_pdus[1]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[1]['DIRECTIVE_CODE']).to eql 'ACK'
        expect(@rx_pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@rx_pdus[2]['DIRECTIVE_CODE']).to eql 'FINISHED'
      end

      it "waits for a closure" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        request(source: 'test1.txt', dest: 'test2.txt', closure: 'CLOSURE_REQUESTED') do |indications|
          # First the transmit indications
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[1]['indication_type']).to eql 'EOF-Sent'
          # Receive indications
          expect(indications[2]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[2]['source_file_name']).to eql 'test1.txt'
          expect(indications[2]['destination_file_name']).to eql 'test2.txt'
          expect(indications[2]['file_size']).to eql 8
          expect(indications[2]['source_entity_id']).to eql 1
          expect(indications[3]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[3]['offset']).to eql 0
          expect(indications[3]['length']).to eql 8
          expect(indications[4]['indication_type']).to eql 'EOF-Recv'
          # 4.6.3.2.3 Notice of completion
          expect(indications[5]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[5]['condition_code']).to eql 'NO_ERROR'
          expect(indications[5]['file_status']).to eql 'FILESTORE_SUCCESS'
          expect(indications[5]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[5]['status_report']).to eql 'FINISHED'
          # Final transmit
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
        expect(@rx_pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@rx_pdus[0]['DIRECTIVE_CODE']).to eql 'FINISHED'
        expect(@rx_pdus[0]['CONDITION_CODE']).to eql 'NO_ERROR'
        expect(@rx_pdus[0]['DELIVERY_CODE']).to eql 'DATA_COMPLETE'
        expect(@rx_pdus[0]['FILE_STATUS']).to eql 'FILESTORE_SUCCESS'
      end

      # 4.6.3.2.2 transaction check timer
      it "times out waiting for a closure" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test1.txt'), data)
        setup(source_id: 1, destination_id: 2)
        CfdpMib.set_entity_value(@source_entity_id, 'check_interval', 0.1)
        request(source: 'test1.txt', dest: 'test2.txt',
                closure: 'CLOSURE_REQUESTED', send_closure: false) do |indications|
          # First the transmit indications
          expect(indications[0]['indication_type']).to eql 'Transaction'
          expect(indications[1]['indication_type']).to eql 'EOF-Sent'
          # Receive indications
          expect(indications[2]['indication_type']).to eql 'Metadata-Recv'
          expect(indications[2]['source_file_name']).to eql 'test1.txt'
          expect(indications[2]['destination_file_name']).to eql 'test2.txt'
          expect(indications[2]['file_size']).to eql 8
          expect(indications[2]['source_entity_id']).to eql 1
          expect(indications[3]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[3]['offset']).to eql 0
          expect(indications[3]['length']).to eql 8
          expect(indications[4]['indication_type']).to eql 'EOF-Recv'
          expect(indications[5]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[5]['condition_code']).to eql 'NO_ERROR'
          expect(indications[5]['file_status']).to eql 'FILESTORE_SUCCESS'
          expect(indications[5]['delivery_code']).to eql 'DATA_COMPLETE'
          expect(indications[5]['status_report']).to eql 'FINISHED'
          # 4.6.3.2.4 Check limit reached fault
          expect(indications[6]['indication_type']).to eql 'Fault'
          expect(indications[6]['condition_code']).to eql 'CHECK_LIMIT_REACHED'
          expect(indications[7]['indication_type']).to eql 'Transaction-Finished'
          expect(indications[7]['condition_code']).to eql 'CHECK_LIMIT_REACHED'
          expect(indications[7]['file_status']).to eql 'UNREPORTED'
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
          source_entity: CfdpMib.entity(@destination_entity_id),
          transaction_seq_num: 1,
          destination_entity: CfdpMib.entity(@source_entity_id),
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

        safe_write_topic("DEFAULT__DECOM__{CFDPTEST}__CFDP_PDU", msg_hash, nil)
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
          expect(indications[3]['file_size']).to eql 8
          expect(indications[3]['source_entity_id']).to eql 1
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
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
          expect(indications[3]['file_size']).to eql 8
          expect(indications[3]['source_entity_id']).to eql 1
          expect(indications[4]['indication_type']).to eql 'File-Segment-Recv'
          expect(indications[4]['offset']).to eql 0
          expect(indications[4]['length']).to eql 8
          expect(indications[5]['indication_type']).to eql 'EOF-Recv'
          # 4.6.1.2.5 Filestore Rejection Fault
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
              file = Tempfile.new('cfdp', binmode: true)
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
              file = Tempfile.new('cfdp', binmode: true)
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

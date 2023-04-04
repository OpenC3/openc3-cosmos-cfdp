require 'rails_helper'
require 'openc3'
require 'openc3/script'
require 'openc3/api/api'
require 'openc3/models/microservice_model'
require 'openc3/utilities/store_autoload'

module OpenC3
  RSpec.describe "cfdp", type: :request do
    describe "POST /cfdp/put" do
      before(:each) do
        mock_redis()
        # setup_system()
        # model = TargetModel.new(folder_name: 'CFDPTEST', name: 'CFDPTEST', scope: "DEFAULT")
        # model.create
        # model.update_store(System.new(['CFDPTEST'], File.join(SPEC_DIR, 'targets')))

        @source_entity_id = 0
        @destination_entity_id = 1
        ENV['OPENC3_MICROSERVICE_NAME'] = 'DEFAULT__API__CFDP'
        # Create the model that is consumed by CfdpMib.setup
        model = MicroserviceModel.new(name: ENV['OPENC3_MICROSERVICE_NAME'], scope: "DEFAULT",
          options: [
            ["source_entity_id", @source_entity_id],
            ["destination_entity_id", @destination_entity_id],
            ["root_path", SPEC_DIR],
            ["tlm_info", "CFDPTEST", "CFDP_PDU", "PDU"],
            ["cmd_info", "CFDPTEST", "CFDP_PDU", "PDU"],
          ],
        )
        model.create
        CfdpMib.setup

        @pdus = []
        allow_any_instance_of(CfdpSourceTransaction).to receive('cmd') do |source, tgt, pkt, params|
          # puts params["PDU"].formatted
          @pdus << CfdpPdu.decom(params["PDU"])
        end
      end

      it "puts a simple text file" do
        data = ('a'..'z').to_a.shuffle[0,8].join
        File.write(File.join(SPEC_DIR, 'test.txt'), data)
        post "/cfdp/put", :params => { scope: "DEFAULT", destination_entity_id: 1, source_file_name: 'test.txt', destination_file_name: 'test.txt' }
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
        expect(@pdus.length).to eql 3
        expect(@pdus[0]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@pdus[0]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@pdus[0]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@pdus[0]['DIRECTIVE_CODE']).to eql 'METADATA'
        expect(@pdus[0]['FILE_SIZE']).to eql data.length

        expect(@pdus[1]['TYPE']).to eql 'FILE_DATA'
        expect(@pdus[1]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@pdus[1]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@pdus[1]['FILE_DATA']).to eql data

        expect(@pdus[2]['TYPE']).to eql 'FILE_DIRECTIVE'
        expect(@pdus[2]['SOURCE_ENTITY_ID']).to eql @source_entity_id
        expect(@pdus[2]['DESTINATION_ENTITY_ID']).to eql @destination_entity_id
        expect(@pdus[2]['DIRECTIVE_CODE']).to eql 'EOF'
        expect(@pdus[2]['CONDITION_CODE']).to eql 'NO_ERROR'
      end
    end
  end
end

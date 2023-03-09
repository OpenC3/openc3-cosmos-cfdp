require 'openc3/models/microservice_model'

CfdpMib.setup
$cfdp_user = CfdpUser.new.start

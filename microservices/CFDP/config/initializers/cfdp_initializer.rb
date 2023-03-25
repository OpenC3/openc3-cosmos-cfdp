require 'cfdp_mib'
require 'cfdp_user'

CfdpMib.setup
$cfdp_user = CfdpUser.new
$cfdp_user.start

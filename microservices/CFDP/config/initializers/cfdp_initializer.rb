require 'cfdp_mib'
require 'cfdp_user'

if ENV['RAILS_ENV'] != 'test'
  CfdpMib.setup
  $cfdp_user = CfdpUser.new
  $cfdp_user.start
end

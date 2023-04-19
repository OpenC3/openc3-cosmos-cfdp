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

require 'cfdp_mib'
require 'cfdp_user'

if ENV['RAILS_ENV'] != 'test'
  CfdpMib.setup
  $cfdp_user = CfdpUser.new
  $cfdp_user.start
else
  $cfdp_user = CfdpUser.new
end

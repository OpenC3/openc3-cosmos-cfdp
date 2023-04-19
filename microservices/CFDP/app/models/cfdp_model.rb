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

require 'openc3/utilities/store'

class CfdpModel
  def self.get_next_transaction_seq_num
    key = "cfdp/#{ENV['OPENC3_MICROSERVICE_NAME']}/transaction_seq_num"
    transaction_seq_num = OpenC3::Store.incr(key)
    return transaction_seq_num
  end
end
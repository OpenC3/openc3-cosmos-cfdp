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

require 'openc3/topics/topic'
require 'openc3/core_ext'

class CfdpTopic < OpenC3::Topic
  def self.write_indication(indication_type, transaction_id:, **kw_args)
    msg_hash = {
      :time => Time.now.to_nsec_from_epoch,
      :indication_type => indication_type,
      :transaction_id => transaction_id
    }
    kw_args.each do |key, value|
      msg_hash[key] = value
    end

    data_hash = {"data" => JSON.generate(msg_hash.as_json(allow_nan: true), allow_nan: true)}
    OpenC3::Topic.write_topic("#{ENV['OPENC3_MICROSERVICE_NAME']}__CFDP", data_hash, '*', 1000)
  end

  def self.read_indications(transaction_id: nil, continuation: nil, limit: 1000)
    continuation = '0-0' unless continuation
    limit = 1000 unless limit
    xread = OpenC3::Topic.read_topics(["#{ENV['OPENC3_MICROSERVICE_NAME']}__CFDP"], [continuation], nil, limit) # Always don't block
    # Return the original continuation and and empty array if we didn't get anything
    indications = []
    return {continuation: continuation, indications: indications} if xread.empty?
    xread.each do |topic, data|
      data.each do |id, msg_hash|
        continuation = id
        msg_hash = JSON.parse(msg_hash["data"], :allow_nan => true, :create_additions => true)
        if !transaction_id or (transaction_id and msg_hash['transaction_id'] == transaction_id)
          indications << msg_hash
        end
      end
    end
    return {continuation: continuation, indications: indications}
  end

  def self.subscribe_indications
    id, _ = OpenC3::Topic.get_newest_message("#{ENV['OPENC3_MICROSERVICE_NAME']}__CFDP")
    return id || '0-0'
  end
end

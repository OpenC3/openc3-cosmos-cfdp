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
    if msg_hash[:filestore_responses]
      msg_hash[:filestore_responses] = JSON.generate(msg_hash[:filestore_responses].as_json(allow_nan: true))
    end
    OpenC3::Topic.write_topic("#{ENV['OPENC3_MICROSERVICE_NAME']}__CFDP", msg_hash, '*', 1000)
  end

  def self.read_indications(transaction_id: nil, continuation: nil, limit: 100)
    continuation = '0-0' unless continuation
    xread = OpenC3::Topic.read_topics(["#{ENV['OPENC3_MICROSERVICE_NAME']}__CFDP"], [continuation], nil, limit) # Always don't block
    # Return the original continuation and and empty array if we didn't get anything
    indications = []
    return {continuation: continuation, indications: indications} if xread.empty?
    xread.each do |topic, data|
      data.each do |id, msg_hash|
        continuation = id
        if !transaction_id or (transaction_id and msg_hash['transaction_id'] == transaction_id)
          if msg_hash["filestore_responses"]
            msg_hash["filestore_responses"] = JSON.parse(msg_hash["filestore_responses"], :allow_nan => true, :create_additions => true)
          end
          indications << msg_hash
        end
      end
    end
    return {continuation: continuation, indications: indications}
  end
end

require 'openc3/utilities/store'

class CfdpModel
  def self.get_next_transaction_seq_num
    key = "cfdp/#{ENV['OPENC3_MICROSERVICE_NAME']}/transaction_seq_num"
    transaction_seq_num = OpenC3::Store.incr(key)
    return transaction_seq_num
  end
end
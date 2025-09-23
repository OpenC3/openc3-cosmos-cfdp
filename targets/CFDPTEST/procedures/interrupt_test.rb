require 'cfdp'

# This script starts a medium size transaction that lasts long enough for the CfdpUser microservice to be restarted
# by Playwright. After the microservice restarts, this script should continue until the transaciton finishes.
# If the transaction fails to resume after the microservice restarts, it should send an 'Abandoned' indication
# (or no indication, at which point Playwright will just time out).

continuation = cfdp_subscribe()
transaction_id = cfdp_put(destination_entity_id: 2, source_file_name: 'medium.bin', destination_file_name: 'interrupt_me.bin', transmission_mode: "ACKNOWLEDGED", timeout: nil)
puts transaction_id

abandoned = false
until abandoned
  error_count = 0
  begin
    indications, continuation = cfdp_indications(continuation: continuation)
    puts indications.inspect
    indications = indications.select { |i| i['transaction_id'] == transaction_id }
    break if indications.any? { |i| i['indication_type'] == 'Transaction-Finished' }
    abandoned = indications.any? { |i| i['indication_type'] == 'Abandoned' }
    wait 1
  rescue => error
    error_count += 1
    raise error if error_count > 2
    puts 'Restart detected'
    puts error.inspect
    wait 5
  end
end
raise 'Transaction abandoned' if abandoned


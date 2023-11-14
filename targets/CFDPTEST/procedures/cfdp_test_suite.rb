require 'openc3/script/suite.rb'
require 'cfdp'
require 'tempfile'

# Group class name should indicate what the scripts are testing
class CfdpTestGroup < OpenC3::Group
  def test_standard_ops
    set_line_delay(0)

    # Simple put and wait for complete
    cfdp_put(destination_entity_id: 2, source_file_name: 'small.bin', destination_file_name: 'small2.bin', transmission_mode: "ACKNOWLEDGED")

    # Start and cancel
    transaction_id = cfdp_put(destination_entity_id: 2, source_file_name: 'medium.bin', destination_file_name: 'medium2.bin', transmission_mode: "ACKNOWLEDGED", timeout: nil)
    wait(1)
    cfdp_cancel(transaction_id: transaction_id)

    # Longer put and interact while running
    continuation = cfdp_subscribe()
    transaction_id = cfdp_put(destination_entity_id: 2, source_file_name: 'medium.bin', destination_file_name: 'medium2.bin', transmission_mode: "ACKNOWLEDGED", timeout: nil)
    wait(1)
    puts cfdp_suspend(transaction_id: transaction_id)
    wait(1)
    puts cfdp_report(transaction_id: transaction_id)
    wait(1)
    puts cfdp_transactions()
    wait(1)
    puts cfdp_resume(transaction_id: transaction_id)
    # Watch the transaction progress
    set_line_delay(0.1)
    done = false
    while true
      indications, continuation = cfdp_indications(continuation: continuation)
      indications.each do |indication|
        puts indication.inspect
        if indication['indication_type'] == 'Transaction-Finished'
          done = true
          break
        end
      end
      #puts cfdp_report(transaction_id: transaction_id)
      puts cfdp_transactions().inspect
      break if done
    end
  end

  def test_proxy_ops
    set_line_delay(0)

    # Simple proxy put and wait for complete
    cfdp_put(remote_entity_id: 2, destination_entity_id: 1, source_file_name: 'small.bin', destination_file_name: 'small2.bin', transmission_mode: "ACKNOWLEDGED")

    # Start and cancel
    transaction_id = cfdp_put(remote_entity_id: 2, destination_entity_id: 1, source_file_name: 'medium.bin', destination_file_name: 'medium2.bin', transmission_mode: "ACKNOWLEDGED", timeout: nil)
    wait(1)
    cfdp_cancel(remote_entity_id: 2, transaction_id: transaction_id)

    puts cfdp_transactions(microservice_name: 'CFDP2', prefix: '/cfdp2', port: 2906)

    # Longer proxy put and interact while running
    continuation = cfdp_subscribe()
    source_transaction_id = cfdp_put(remote_entity_id: 2, destination_entity_id: 1, source_file_name: 'medium.bin', destination_file_name: 'medium2.bin', transmission_mode: "ACKNOWLEDGED", timeout: nil)
    wait(1)

    # Get the most recent transaction id from the other entity and assume that is the right one...
    # The CFDP spec is lacking here
    transactions = cfdp_transactions(microservice_name: 'CFDP2', prefix: '/cfdp2', port: 2906)
    transaction_id = transactions[-1]['id']
    puts cfdp_suspend(remote_entity_id: 2, transaction_id: transaction_id)
    wait(1)
    puts cfdp_report(remote_entity_id: 2, transaction_id: transaction_id, report_file_name: "myreport.txt")
    wait(1)
    puts cfdp_transactions(microservice_name: 'CFDP2', prefix: '/cfdp2', port: 2906)
    wait(1)
    puts cfdp_resume(remote_entity_id: 2, transaction_id: transaction_id)
    # Watch the transaction progress
    set_line_delay(0)
    done = false
    while true
      indications, continuation = cfdp_indications(continuation: continuation)
      indications.each do |indication|
        puts indication.inspect unless indication['indication_type'] == 'File-Segment-Recv'
        if indication['indication_type'] == 'Proxy-Put-Response' and indication['transaction_id'] == source_transaction_id
          done = true
          break
        end
      end
      wait(0.1)
      #puts cfdp_report(transaction_id: transaction_id)
      puts cfdp_transactions(microservice_name: 'CFDP2', prefix: '/cfdp2', port: 2906)
      break if done
    end
  end

  def setup
    set_line_delay(0)

    # Create test files
    data = "\x00" * 1000

    # small.bin
    file = Tempfile.new('cfdp', binmode: true)
    1000.times do
      file.write(data)
    end
    file.rewind
    put_target_file("/CFDP/tmp/small.bin", file)
    file.unlink

    # medium.bin
    file = Tempfile.new('cfdp', binmode: true)
    10000.times do
      file.write(data)
    end
    file.rewind
    put_target_file("/CFDP/tmp/medium.bin", file)
    file.unlink
  end

  def teardown
    delete_target_file("/CFDP/tmp/small.bin")
    delete_target_file("/CFDP/tmp/medium.bin")
  end
end

class CfdpTestSuite < OpenC3::Suite
  def initialize
    add_group('CfdpTestGroup')
  end
end

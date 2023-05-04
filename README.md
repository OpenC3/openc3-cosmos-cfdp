# OpenC3 COSMOS CFDP Plugin

This plugin provides a microservice and corresponding API for handling the CCSDS File Delivery Protocol (CFDP).

This CFDP implementation is intended to be compliant with CCSDS 727.0-B-5 - Blue Book July 2020.

## Installation

1. Install this plugin in the Admin Tool
2. During installation, edit plugin.txt to configure all of your MIB settings (See MIB Configuration Below)

## Usage in Scripts

See: [cfdp.rb]()

```
require 'cfdp'

# Send a file and wait up to 10 minutes for complete (Default mode)
transaction_id, indication = cfdp_put(destination_entity_id: 1, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin")

# Force UNACKNOWLEDGED mode
transaction_id, indication = cfdp_put(destination_entity_id: 1, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin", transmission_mode: "UNACKNOWLEDGED")

# Force UNACKNOWLEDGED mode and request closure
transaction_id, indication = cfdp_put(destination_entity_id: 1, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin", transmission_mode: "UNACKNOWLEDGED", closure_requested: "CLOSURE_REQUESTED")

# Force ACKNOWLEDGED mode
transaction_id, indication = cfdp_put(destination_entity_id: 1, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin", transmission_mode: "ACKNOWLEDGED")

# Send a file and don't wait
transaction_id = cfdp_put(destination_entity_id: 1, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin", timeout: nil)
...
DO_OTHER_THINGS
...
indication = cfdp_indications(transaction_id: transaction_id, indication_type: 'Transaction-Finished')

# Cancel a transaction
cfdp_cancel(transaction_id: transaction_id)

# Suspend a transaction
cfdp_suspend(transaction_id: transaction_id)

# Resume a suspended transaction
cfdp_resume(transaction_id: transaction_id)

# Get a report on a transaction
indication = cfdp_report(transaction_id: transaction_id)
puts indication["status_report"]

# Filestore requests
requests = []
requests << ["CREATE_FILE", file_name]
requests << ["DELETE_FILE", file_name]
requests << ["RENAME_FILE", old_file_name, new_file_name]
requests << ["APPEND_FILE", file_name, appended_file_name]
requests << ["REPLACE_FILE", replaced_file_name, contents_file_name]
requests << ["CREATE_DIRECTORY", directory_name]
requests << ["REMOVE_DIRECTORY", directory_name]
requests << ["DENY_FILE", file_name]
requests << ["DENY_DIRECTORY", directory_name]
transaction_id, indication = cfdp_put(destination_entity_id: 1, filestore_requests: requests)
indication["filestore_responses"].each do |response|
  puts "Filestore failed: #{response}" if response["STATUS_CODE"] != "SUCCESSFUL"
end

# Proxy operations
# These add remote_entity_id to specify a remote entity

# Get a file (using proxy put) and wait for it to be received
remote_entity_id = 1
my_entity_id = 2
cfdp_put(remote_entity_id: remote_entity_id, destination_entity_id: my_entity_id, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin")

# Cancel a proxy put transaction
cfdp_cancel(transaction_id: transaction_id, remote_entity_id: 1)

# Suspend a remote transaction
cfdp_suspend(transaction_id: transaction_id, remote_entity_id: 1)

# Resume a remote transaction
cfdp_resume(transaction_id: transaction_id, remote_entity_id: 1)

# Get a status report on a remote transaction
cfdp_report(transaction_id: transaction_id, remote_entity_id: 1, report_file_name: "my_report.txt")

# Get a remote directory listing
cfdp_directory_listing(remote_entity_id: 1, directory_name: "/files", directory_file_name: "my_listing.txt")

```

## MIB Configuration

## Known Limitations

1. Segmentation Control and Flow Label have no effect
2. Suspension requests are not queued as specified in 6.5.4.1.2
3. Annex B - Store and Forward Overlay Operations are not implemented

## Contributing

We encourage you to contribute to OpenC3 COSMOS and this project!

Contributing is easy.

1. Fork the project
2. Create a feature branch
3. Make your changes
4. Submit a pull request

YOU MUST AGREE TO OUR CONTRIBUTOR LICENSE AGREEMENT TO SUBMIT CODE TO THIS PROJECT: See [CONTRIBUTING.txt](CONTRIBUTING.txt)

Most importantly:

FOR ALL CONTRIBUTIONS TO THE OPENC3 COSMOS PROJECT AND ASSOCIATED PLUGINS, OPENC3, INC. MAINTAINS ALL RIGHTS TO ALL CODE CONTRIBUTED INCLUDING THE RIGHT TO LICENSE IT UNDER OTHER TERMS.

## License

OpenC3 Evaluation and Educational License

See [LICENSE.txt](LICENSE.txt)

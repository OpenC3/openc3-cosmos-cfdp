# OpenC3 COSMOS CFDP Plugin

This plugin provides a microservice and corresponding API for handling the CCSDS File Delivery Protocol (CFDP).

This CFDP implementation is intended to be compliant with:

- CCSDS 727.0-B-5 - Blue Book July 2020
- CCSDS 727.0-B-4 - Blue Book January 2007
- CCSDS 727.0-B-3 - Blue Book June 2005

It is also potentially compliant with earlier versions but that has not been evaluated.

## Installation

1. Install this plugin in the Admin Tool
2. During installation, edit plugin.txt to configure all of your MIB settings (See MIB Configuration Below)

In particular be sure to set the desired protocol_version_number for each remote entity. protocol_version_number 0 is used for all versions of the standard before the 2020 release. protocol_version_number 1 is used for the current release and potential future releases. This plugin defaults to using protocol_version_number 1.

## Usage in Scripts

See: [cfdp.rb](./lib/cfdp.rb)

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
continuation = cfdp_subscribe()
transaction_id = cfdp_put(destination_entity_id: 1, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin", timeout: nil)
...
DO_OTHER_THINGS
...
indication = cfdp_indications(transaction_id: transaction_id, indication_type: 'Transaction-Finished', continuation: continuation)

# Cancel a transaction
indication = cfdp_cancel(transaction_id: transaction_id)

# Suspend a transaction
indication = cfdp_suspend(transaction_id: transaction_id)

# Resume a suspended transaction
indication = cfdp_resume(transaction_id: transaction_id)

# Get a report on a transaction
indication = cfdp_report(transaction_id: transaction_id)
puts indication["status_report"]

# Get a list of transactions
transactions = cfdp_transactions(active: true)

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
transaction_id, indication = cfdp_put(remote_entity_id: remote_entity_id, destination_entity_id: my_entity_id, source_file_name: "file_to_send.bin", destination_file_name: "received_file.bin")

# Cancel a proxy put transaction
indication = cfdp_cancel(transaction_id: transaction_id, remote_entity_id: 1)

# Suspend a remote transaction
indication = cfdp_suspend(transaction_id: transaction_id, remote_entity_id: 1)

# Resume a remote transaction
indication = cfdp_resume(transaction_id: transaction_id, remote_entity_id: 1)

# Get a status report on a remote transaction
indication = cfdp_report(transaction_id: transaction_id, remote_entity_id: 1, report_file_name: "my_report.txt")

# Get a remote directory listing
indications = cfdp_directory_listing(remote_entity_id: 1, directory_name: "/files", directory_file_name: "my_listing.txt")

```

### `cfdp_put_dir`

This is a conveninence method that allows you to easily send multiple files. This operation is not part of the CFDP specification, so it is implemented by initiating a separate PUT request for each file in the given directory. It is used the same way as `cfdp_put()`, but you must pass the path to a directory instead of a file (e.g. `"/tmp"` instead of `"/tmp/foo.txt"`).

## MIB Configuration

The CFDP Management Information Base (MIB) is configured by passing options to the CFDP microservice in plugin.txt. See the local [plugin.txt](plugin.txt), for example:

    MICROSERVICE CFDP CFDP
      ...
      # MIB Options Follow - Modify for your CFDP implementation!
      OPTION root_path /DEFAULT/targets_modified/CFDP/tmp
      OPTION bucket config

The MIB Options will always need to be configured for your CFDP implementation and mission. The [plugin.txt](plugin.txt) is an example which includes a test implementation. This can be edited on installation in COSMOS (click the plugin.txt tab) but can also be edited and built locally.

Settings fall into three groups:

1. **Global Configuration** - Engine-wide settings that have a single value for the whole CFDP microservice. They are not associated with any entity.
2. **Local (Source) Entity Configuration** - Settings for this COSMOS engine's own entity, declared with `source_entity_id`. They govern indications, fault handling, telemetry reception, and transaction retention.
3. **Remote (Destination) Entity Configuration** - Per-peer settings declared with `destination_entity_id`. They govern the CFDP protocol behavior used to communicate with each remote entity, both when sending files to it and when receiving files from it.

Every per-entity setting is applied to the most recently declared `source_entity_id` or `destination_entity_id`. Internally, the same set of fields exists on every entity (the parser does not prevent a "remote" setting from being placed on the source entity or vice versa), so the groupings below reflect which entity the engine actually reads each setting from at runtime.

Minimum required settings:

- A source_entity_id and corresponding tlm_info must be given.
- At least one destination_entity_id must be defined with a corresponding cmd_info.
- root_path must be defined
- bucket should be set if the root_path is in a bucket. Otherwise the root path is assumed to be a mounted volume.

### Global Configuration

These settings are applied to the CFDP microservice via `OPTION <name> <value>` in the [plugin.txt](plugin.txt) and apply to the entire engine, independent of any entity. They can be specified anywhere in the options list.

| Setting Name                        | Description                                                                                       | Allowed Values    | Default Value                   |
| ----------------------------------- | ------------------------------------------------------------------------------------------------- | ----------------- | ------------------------------- |
| root_path                           | The path to send/receive files from                                                               | Valid directory   | N/A - Must be given             |
| bucket                              | The bucket to send/receive files from                                                             | Valid bucket Name | nil - Serve from mounted volume |
| prevent_received_file_overwrite     | Appends a timestamp to the file name for received files if the file already exists                | true or false     | true                            |
| allow_duplicate_transaction_ids     | Allows receiving transactions with an ID that was previously used by deleting the old transaction | true or false     | false                           |
| transaction_cleanup_frequency_hours | How often to purge old (completed) transactions from RAM and Redis                                | Any integer       | 24                              |

> Note: `plugin_test_mode` is a plugin.txt build `VARIABLE` (not a MIB `OPTION`). When set true it installs two dummy entities/targets that can send/receive transactions to each other for testing. It is consumed when the plugin is built, not by the CFDP microservice.

### Local (Source) Entity Configuration

These settings are applied via `OPTION <name> <value>` after declaring the `source_entity_id`. They configure this COSMOS engine's own (local) entity. At runtime they are read from the local entity regardless of whether a transaction is being sent or received.

| Setting Name                    | Description                                                                                        | Allowed Values                                                                                                                                                                                                                                                                                                                                                        | Default Value       |
| ------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| source_entity_id                | The entity id for this CFDP microservice                                                          | Any integer                                                                                                                                                                                                                                                                                                                                                         | N/A - Must be given |
| tlm_info                        | A target_name, packet_name, and item_name to receive PDUs. Multiple tlm_info options can be given | COSMOS packet information                                                                                                                                                                                                                                                                                                                                           | N/A - Must be given |
| eof_sent_indication             | Issue EOF-Sent.indication                                                                         | true or false                                                                                                                                                                                                                                                                                                                                                       | true                |
| eof_recv_indication             | Issue EOF-Recv.indication                                                                         | true or false                                                                                                                                                                                                                                                                                                                                                       | true                |
| file_segment_recv_indication    | Issue File-Segment-Recv.indication                                                                | true or false                                                                                                                                                                                                                                                                                                                                                       | true                |
| transaction_finished_indication | Issue Transaction-Finished.indication                                                             | true or false                                                                                                                                                                                                                                                                                                                                                       | true                |
| suspended_indication            | Issue Suspended.indication                                                                        | true or false                                                                                                                                                                                                                                                                                                                                                       | true                |
| resume_indication               | Issue Resume.indication                                                                           | true or false                                                                                                                                                                                                                                                                                                                                                       | true                |
| enable_eof_nak                  | Send a NAK in response to a received EOF. Only the local (receiving) entity's value is used.      | true or false                                                                                                                                                                                                                                                                                                                                                       | false               |
| transaction_closure_requested   | Default closure requested setting used when this engine sends a file. Read from the local entity. | CLOSURE_REQUESTED or CLOSURE_NOT_REQUESTED                                                                                                                                                                                                                                                                                                                          | CLOSURE_REQUESTED   |
| fault_handler                   | Fault handler setting. Only the local (source) entity's fault handlers are consulted.             | (ACK_LIMIT_REACHED, KEEP_ALIVE_LIMIT_REACHED, INVALID_TRANSMISSION_MODE, FILESTORE_REJECTION, FILE_CHECKSUM_FAILURE, FILE_SIZE_ERROR, NAK_LIMIT_REACHED, INACTIVITY_DETECTED, INVALID_FILE_STRUCTURE, CHECK_LIMIT_REACHED, or UNSUPPORTED_CHECKSUM_TYPE) followed by (ISSUE_NOTICE_OF_CANCELLATION, ISSUE_NOTICE_OF_SUSPENSION, IGNORE_ERROR, or ABANDON_TRANSACTION) | See Code            |
| transaction_retain_seconds      | Time to keep completed transactions in seconds. Only the source entity's value is used.           | Floating point value greater than 0                                                                                                                                                                                                                                                                                                                                 | 86400               |

### Remote (Destination) Entity Configuration

These settings are applied via `OPTION <name> <value>` after declaring a `destination_entity_id`. They configure a remote peer (Bus or Payload Flight Software) and are read whenever this engine communicates with that peer - both when sending files to it and when receiving files from it.

| Setting Name                  | Description                                                                                          | Allowed Values                            | Default Value             |
| ----------------------------- | --------------------------------------------------------------------------------------------------- | ----------------------------------------- | ------------------------- |
| destination_entity_id         | Id of a remote entity to configure                                                                  | Any integer                               | N/A - Must be given       |
| cmd_info                      | The target_name, packet_name, and item_name to send PDUs for the destination entity                 | COSMOS packet information                 | N/A - Must be given       |
| protocol_version_number       | CFDP Version Number Needed at Destination                                                           | 0 or 1                                    | 1 - CFDP Blue Book Rev 5+ |
| ack_timer_interval            | Ack timeout in seconds                                                                               | Any integer                               | 600 seconds               |
| nak_timer_interval            | Nak timeout in seconds                                                                               | Any integer                               | 600 seconds               |
| maximum_file_segment_length   | Maximum amount of file data in a segment in bytes                                                   | Any integer                               | 1024 bytes                |
| ack_timer_expiration_limit    | Number of times to wait for the ack timeout before declaring a fault                                | Any integer                               | 1                         |
| nak_timer_expiration_limit    | Number of times to wait for the nak timeout before declaring a fault                                | Any integer                               | 1                         |
| transaction_inactivity_limit  | Number of times to wait for the keep alive timeout before declaring the transaction inactive fault  | Any integer                               | 1                         |
| check_limit                   | Number of times to check for transaction complete before declaring a fault                          | Any integer                               | 1                         |
| keep_alive_discrepancy_limit  | Maximum difference between keep alive progress and source progress allowed before declaring a fault. | Any integer                               | 1024000 bytes             |
| enable_acks                   | Send Acks in Acknowledged mode                                                                      | true or false                             | true                      |
| enable_keep_alive             | Send Keep Alives in Acknowledged mode                                                               | true or false                             | true                      |
| enable_finished               | Send Finished PDU if closure requested or acknowledged mode                                         | true or false                             | true                      |
| default_transmission_mode     | Default put mode                                                                                    | ACKNOWLEDGED or UNACKNOWLEDGED            | UNACKNOWLEDGED            |
| entity_id_length              | Size of entity ids in bytes minus one                                                               | 0 to 7                                    | 0 = 1 byte                |
| sequence_number_length        | Size of sequence numbers in bytes minus one                                                         | 0 to 7                                    | 0 = 1 byte                |
| default_checksum_type         | Checksum type number                                                                                | 0 to 15                                   | 0 = Default CFDP checksum |
| incomplete_file_disposition   | What to do with an incomplete file                                                                  | DISCARD or RETAIN                         | DISCARD                   |
| cmd_delay                     | Delay after sending each PDU in seconds. Defaults to no delay.                                      | Floating point value greater than 0       | nil                       |

### Settings That Apply to Both Entities

The following settings are read from the local (source) entity in one role and from a remote (destination) entity in the other, so for predictable behavior they should be configured consistently on both your `source_entity_id` and each `destination_entity_id`:

| Setting Name        | Description                                            | Allowed Values | Default Value | Read from local entity                              | Read from remote entity                              |
| ------------------- | ----------------------------------------------------- | -------------- | ------------- | --------------------------------------------------- | ---------------------------------------------------- |
| crcs_required       | Whether PDUs include/require a CRC                     | true or false  | true          | When decoding any received PDU                      | When building PDUs to send to that entity            |
| check_interval      | Interval to check for transaction complete in seconds | Any integer    | 600 seconds   | When sending (waiting for closure after EOF)        | When receiving (check timer for completion)          |
| keep_alive_interval | Keep Alive Period in seconds                          | Any integer    | 600 seconds   | When resuming a transaction (inactivity timeout)    | When receiving (keep alive / inactivity timers)      |
| immediate_nak_mode  | Send NAKs as soon as something is noticed missing     | true or false  | true          | When receiving, on EOF-triggered NAK decisions      | When receiving, on file-data-triggered NAK decisions |

## Known Limitations

1. Segmentation Control and Flow Label have no effect
2. Suspension requests are not queued as specified in 6.5.4.1.2
3. Annex B - Store and Forward Overlay Operations are not implemented
4. Extended operations and classes 3 and 4 from the earlier CFDP standards are not implemented

## Contributing

We encourage you to contribute to OpenC3 COSMOS and this project!

Contributing is easy.

1. Fork the project
2. Create a feature branch
3. Make your changes
4. Submit a pull request

By contributing to this project, you agree to the following terms in our OpenC3 Builder License: [LICENSE.md](https://github.com/OpenC3/cosmos/blob/main/LICENSE.md)

## License

OpenC3 Evaluation and Educational License

See [LICENSE.md](LICENSE.md)

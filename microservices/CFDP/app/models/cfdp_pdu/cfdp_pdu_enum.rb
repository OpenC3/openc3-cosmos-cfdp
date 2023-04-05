# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpPdu < OpenC3::Packet
  # Table 5-4: Directive Codes
  DIRECTIVE_CODES = {
    "EOF" => 4,
    "FINISHED" => 5,
    "ACK" => 6,
    "METADATA" => 7,
    "NAK" => 8,
    "PROMPT" => 9,
    "KEEP_ALIVE" => 0x0C
    # 0D-FF are Reserved
  }

  # Table 5-5: Condition Codes
  CONDITION_CODES = {
    "NO_ERROR" => 0,
    "ACK_LIMIT_REACHED" => 1,
    "KEEP_ALIVE_LIMIT_REACHED" => 2,
    "INVALID_TRANSMISSION_MODE" => 3,
    "FILESTORE_REJECTION" => 4,
    "FILE_CHECKSUM_FAILURE" => 5,
    "FILE_SIZE_ERROR" => 6,
    "NAK_LIMIT_REACHED" => 7,
    "INACTIVITY_DETECTED" => 8,
    "INVALID_FILE_STRUCTURE" => 9,
    "CHECK_LIMIT_REACHED" => 10,
    "UNSUPPORTED_CHECKSUM_TYPE" => 11,
    "SUSPEND_REQUEST_RECEIVED" => 14,
    "CANCEL_REQUEST_RECEIVED" => 15
  }

  # Table 5-7: Finished PDU Contents
  DELIVERY_CODES = {
    "DATA_COMPLETE" => 0,
    "DATA_INCOMPLETE" => 1
  }

  # Table 5-7: Finished PDU Contents
  FILE_STATUS_CODES = {
    "FILE_DISCARDED" => 0,
    "FILESTORE_REJECTION" => 1,
    "FILESTORE_SUCCESS" => 2,
    "UNREPORTED" => 3
  }

  # Paragraph 5.2.4 ACK PDU
  TRANSACTION_STATUS_CODES = {
    "UNDEFINED" => 0,
    "ACTIVE" => 1,
    "TERMINATED" => 2,
    "UNRECOGNIZED" => 3
  }

  # Table 5-16: Filestore Request TLV Action Codes
  ACTION_CODES = {
    "CREATE_FILE" => 0,
    "DELETE_FILE" => 1,
    "RENAME_FILE" => 2,
    "APPEND_FILE" => 3,
    "REPLACE_FILE" => 4,
    "CREATE_DIRECTORY" => 5,
    "REMOVE_DIRECTORY" => 6,
    "DENY_FILE" => 7,
    "DENY_DIRECTORY" => 8
  }

  # Table 5-18: Filestore Response Status Codes
  CREATE_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "NOT_ALLOWED" => 1,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  DELETE_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "FILE_DOES_NOT_EXIST" => 1,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  RENAME_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "OLD_FILE_DOES_NOT_EXIST" => 1,
    "NEW_FILE_ALREADY_EXISTS" => 2,
    "NOT_ALLOWED" => 3,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  APPEND_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "FILE_1_DOES_NOT_EXIST" => 1,
    "FILE_2_DOES_NOT_EXIST" => 2,
    "NOT_ALLOWED" => 3,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  REPLACE_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "FILE_1_DOES_NOT_EXIST" => 1,
    "FILE_2_DOES_NOT_EXIST" => 2,
    "NOT_ALLOWED" => 3,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  CREATE_DIRECTORY_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "CANNOT_BE_CREATED" => 1,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  REMOVE_DIRECTORY_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "DOES_NOT_EXIST" => 1,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  DENY_FILE_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  DENY_DIRECTORY_STATUS_CODES = {
    "SUCCESSFUL" => 0,
    "NOT_ALLOWED" => 2,
    "NOT_PERFORMED" => 15
  }

  # Special for unknown action codes
  UNKNOWN_STATUS_CODES = {
    "NOT_PERFORMED" => 15
  }

  # Table 5-18: Filestore Response Status Codes
  FILESTORE_RESPONSE_STATUS_CODES = {
    "CREATE_FILE" => CREATE_FILE_STATUS_CODES,
    "DELETE_FILE" => DELETE_FILE_STATUS_CODES,
    "RENAME_FILE" => RENAME_FILE_STATUS_CODES,
    "APPEND_FILE" => APPEND_FILE_STATUS_CODES,
    "REPLACE_FILE" => REPLACE_FILE_STATUS_CODES,
    "CREATE_DIRECTORY" => CREATE_DIRECTORY_STATUS_CODES,
    "REMOVE_DIRECTORY" => REMOVE_DIRECTORY_STATUS_CODES,
    "DENY_FILE" => DENY_FILE_STATUS_CODES,
    "DENY_DIRECTORY" => DENY_DIRECTORY_STATUS_CODES
  }

  # Table 5-14: File Data PDU Contents
  RECORD_CONTINUATION_STATES = {
    "NEITHER_START_NOR_END" => 0,
    "START" => 1,
    "END" => 2,
    "START_AND_END" => 3
  }

  # Defined in Section 5.4
  TLV_TYPES = {
    "FILESTORE_REQUEST" => 0,
    "FILESTORE_RESPONSE" => 1,
    "MESSAGE_TO_USER" => 2,
    "FAULT_HANDLER_OVERRIDE" => 4,
    "FLOW_LABEL" => 5,
    "ENTITY_ID" => 6
  }

  # Table 5-19
  HANDLER_CODES = {
    "ISSUE_NOTICE_OF_CANCELATION" => 1,
    "ISSUE_NOTICE_OF_SUSPENSION" => 2,
    "IGNORE_ERROR" => 3,
    "ABONDON_TRANSACTION" => 4
  }
end

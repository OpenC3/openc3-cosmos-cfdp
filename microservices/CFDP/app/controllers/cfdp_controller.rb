# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

class CfdpController < ApplicationController

  # Put.request (destination CFDP entity ID,
  #   [source file name],
  #   [destination file name],
  #   [segmentation control],
  #   [fault handler overrides],
  #   [flow label],
  #   [transmission mode],
  #   [closure requested],
  #   [messages to user],
  #   [filestore requests])
  def put
    Thread.new do
      CfdpProcedures.put(
        destination_entity_id: params[:destination_entity_id],
        source_file_name: params[:source_file_name],
        destination_file_name: params[:destination_file_name]
      )
    end
  end

  # Cancel.request (transaction ID)
  def cancel

  end

  # Suspend.request (transaction ID)
  def suspend

  end

  # Resume.request (transaction ID)
  def resume

  end

  # Report.request (transaction ID)
  def report

  end

  # Transaction.indication (transaction ID)
  # EOF-Sent.indication (transaction ID)
  # Transaction-Finished.indication (transaction ID,
  #    [filestore responses],
  #    [status report],
  #    condition code,
  #    file status,
  #    delivery code)
  # Metadata-Recv.indication (transaction ID,
  #    source CFDP entity ID,
  #    [file size],
  #    [source file name],
  #    [destination file name],
  #    [messages to user])
  # File-Segment-Recv.indication (transaction ID,
  #   offset,
  #   length,
  #   [record continuation state,
  #   length of segment metadata,
  #   segment metadata])
  # Suspended.indication (transaction ID,
  #    condition code)
  # Resumed.indication (transaction ID,
  #    progress)
  # Report.indication (transaction ID,
  #    status report)
  # Fault.indication (transaction ID,
  #    condition code,
  #    progress)
  # Abandoned.indication (transaction ID,
  #    condition code,
  #    progress)
  # EOF-Recv.indication (transaction ID)
  def indications

  end
end

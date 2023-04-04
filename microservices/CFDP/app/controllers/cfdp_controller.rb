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
    return unless authorization('cmd')
    params.require([:destination_entity_id, :source_file_name, :destination_file_name])
    transaction = CfdpSourceTransaction.new
    Thread.new do
      begin
        transaction.put(
          destination_entity_id: params[:destination_entity_id],
          source_file_name: params[:source_file_name],
          destination_file_name: params[:destination_file_name],
          closure_requested: params[:closure_requested],
        )
      rescue => err
        OpenC3::Logger.error(err.formatted, scope: ENV['OPENC3_SCOPE'])
      end
    end
    render json: transaction.id
  rescue ActionController::ParameterMissing => error
    render :json => { :status => 'error', :message => error.message }, :status => 400
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
    return unless authorization('cmd')
    result = CfdpTopic.read_indications(transaction_id: params[:transaction_id], continuation: params[:continuation], limit: params[:limit])
    render json: result
  end
end

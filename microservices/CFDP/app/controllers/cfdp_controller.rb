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

class CfdpController < ApplicationController
  # Put.request (destination CFDP entity ID,
  #   [source file name],
  #   [destination file name],
  #   [segmentation control], # Not supported
  #   [fault handler overrides],
  #   [flow label], # Not supported
  #   [transmission mode],
  #   [closure requested],
  #   [messages to user],
  #   [filestore requests])
  def put
    return unless authorization('cmd')
    params.require([:destination_entity_id])
    if params[:destination_entity_id].to_i.to_s != params[:destination_entity_id].to_s
      render :json => { :status => 'error', :message => "destination_entity_id must be numeric" }, :status => 400
      return
    end
    transaction = $cfdp_user.start_source_transaction(params)
    render json: transaction.id
  rescue ActionController::ParameterMissing => error
    render :json => { :status => 'error', :message => error.message }, :status => 400
  end

  # Cancel.request (transaction ID)
  def cancel
    return unless authorization('cmd')
    params.require([:transaction_id])
    entity_id = params[:entity_id]
    transaction = CfdpMib.transactions[params[:transaction_id]]
    if transaction
      transaction.cancel(entity_id)
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  end

  # Suspend.request (transaction ID)
  def suspend
    return unless authorization('cmd')
    params.require([:transaction_id])
    transaction = CfdpMib.transactions[params[:transaction_id]]
    if transaction
      transaction.suspend
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  end

  # Resume.request (transaction ID)
  def resume
    return unless authorization('cmd')
    params.require([:transaction_id])
    transaction = CfdpMib.transactions[params[:transaction_id]]
    if transaction
      transaction.resume
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  end

  # Report.request (transaction ID)
  def report
    return unless authorization('cmd')
    params.require([:transaction_id])
    transaction = CfdpMib.transactions[params[:transaction_id]]
    if transaction
      transaction.report
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  end

  def directory_listing
    return unless authorization('cmd')
    params.require([:entity_id, :directory_name, :directory_file_name])
    if params[:entity_id].to_i.to_s != params[:entity_id].to_s
      render :json => { :status => 'error', :message => "entity_id must be numeric" }, :status => 400
      return
    end
    transaction = $cfdp_user.start_directory_listing(params)
    render json: transaction.id
  rescue ActionController::ParameterMissing => error
    render :json => { :status => 'error', :message => error.message }, :status => 400
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

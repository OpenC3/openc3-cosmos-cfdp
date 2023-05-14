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

require 'openc3/config/config_parser'

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
    params.require([:destination_entity_id])
    return unless check_authorization()
    transaction = $cfdp_user.start_source_transaction(params)
    render json: transaction.id
  rescue ActionController::ParameterMissing => error
    render :json => { :status => 'error', :message => error.message }, :status => 400
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  # Cancel.request (transaction ID)
  def cancel
    params.require([:transaction_id])
    return unless check_authorization()
    transaction = $cfdp_user.cancel(params)
    if transaction
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  # Suspend.request (transaction ID)
  def suspend
    params.require([:transaction_id])
    return unless check_authorization()
    transaction = $cfdp_user.suspend(params)
    if transaction
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  # Resume.request (transaction ID)
  def resume
    params.require([:transaction_id])
    return unless check_authorization()
    transaction = $cfdp_user.resume(params)
    if transaction
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  # Report.request (transaction ID)
  def report
    params.require([:transaction_id])
    return unless check_authorization()
    transaction = $cfdp_user.report(params)
    if transaction
      render json: transaction.id
    else
      render :json => { :status => 'error', :message => "Transaction #{params[:transaction_id]} not found" }, :status => 404
    end
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  def directory_listing
    params.require([:remote_entity_id, :directory_name, :directory_file_name])
    return unless check_authorization()
    transaction = $cfdp_user.start_directory_listing(params)
    render json: transaction.id
  rescue ActionController::ParameterMissing => error
    render :json => { :status => 'error', :message => error.message }, :status => 400
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  def subscribe
    return unless check_authorization()
    result = CfdpTopic.subscribe_indications
    render json: result
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
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
    return unless check_authorization()
    result = CfdpTopic.read_indications(transaction_id: params[:transaction_id], continuation: params[:continuation], limit: params[:limit])
    render json: result
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  def transactions
    return unless check_authorization()
    active = false
    active = true if OpenC3::ConfigParser.handle_true_false_nil(params[:active])
    result = []
    transactions = CfdpMib.transactions.dup
    transactions.each do |t_id, t|
      if not active or t.transaction_status == "ACTIVE"
        result << t
      end
    end
    result = result.sort {|a, b| a.id <=> b.id}
    render json: result
  rescue => error
    render :json => { :status => 'error', :message => error.message }, :status => 500
  end

  # private

  def check_authorization
    cmd_entity_id = nil
    cmd_entity = nil

    if params[:remote_entity_id]
      if params[:remote_entity_id].to_i.to_s != params[:remote_entity_id].to_s
        render :json => { :status => 'error', :message => "remote_entity_id must be numeric" }, :status => 400
        return false
      end
      cmd_entity_id = Integer(params[:remote_entity_id])
      cmd_entity = CfdpMib.entity(cmd_entity_id)
    elsif params[:destination_entity_id]
      if params[:destination_entity_id].to_i.to_s != params[:destination_entity_id].to_s
        render :json => { :status => 'error', :message => "destination_entity_id must be numeric" }, :status => 400
        return
      end
      cmd_entity_id = Integer(params[:destination_entity_id])
      cmd_entity = CfdpMib.entity(cmd_entity_id)
    else
      cmd_entity_id = CfdpMib.source_entity_id
      cmd_entity = CfdpMib.entity(cmd_entity_id)
    end

    if cmd_entity
      target_name, packet_name, item_name = cmd_entity["cmd_info"]
      unless target_name and packet_name and item_name
        tlm_packets = cmd_entity["tlm_info"]
        tlm_packets ||= []
        target_name, packet_name, item_name = tlm_packets[0]
      end
      if target_name and packet_name and item_name
        # Caller must be able to send this command
        return false unless authorization('cmd', target_name: target_name, packet_name: packet_name)
      else
        render :json => { :status => 'error', :message => "info not configured for entity: #{cmd_entity_id}" }, :status => 400
        return false
      end
    else
      render :json => { :status => 'error', :message => "Unknown entity: #{cmd_entity_id}" }, :status => 400
      return false
    end

    # Authorized
    return true
  end

end

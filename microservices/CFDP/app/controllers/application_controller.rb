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

require 'openc3/utilities/authorization'

class ApplicationController < ActionController::API
  include OpenC3::Authorization

  private

  # Authorize and rescue the possible execeptions
  # @return [Boolean] true if authorize successful
  def authorization(permission)
    begin
      authorize(
        permission: permission,
        scope: params[:scope],
        token: request.headers['HTTP_AUTHORIZATION'],
      )
    rescue OpenC3::AuthError => e
      render(json: { status: 'error', message: e.message }, status: 401) and
        return false
    rescue OpenC3::ForbiddenError => e
      render(json: { status: 'error', message: e.message }, status: 403) and
        return false
    end
    true
  end
end

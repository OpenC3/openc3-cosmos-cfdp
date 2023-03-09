# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# if purchased from OpenC3, Inc.

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

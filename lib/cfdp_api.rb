# encoding: ascii-8bit

# Copyright 2022 OpenC3, Inc.
# All Rights Reserved.
#
# This file may only be used under the terms of a commercial license
# purchased from OpenC3, Inc.

require 'openc3/utilities/authentication'
require 'openc3/io/json_api_object'

# This should get moved to base COSMOS
module OpenC3
  class JsonApi
    # Create a JsonApiObject connection to the API server
    def initialize(microservice_name: 'CFDP', prefix: '/cfdp', schema: 'http', hostname: nil, port: 2905, timeout: 5.0, url: nil)
      url = _generate_url(microservice_name: microservice_name, prefix: prefix, schema: schema, hostname: hostname, port: port) unless url
      @json_api = OpenC3::JsonApiObject.new(
        url: url,
        timeout: timeout,
        authentication: _generate_auth()
      )
    end

    def shutdown
      @json_api.shutdown
    end

    # private

    # pull openc3-cosmos-script-runner-api url from environment variables
    def _generate_url(microservice_name: 'CFDP', prefix: '/cfdp', schema: 'http', hostname: nil, port: 2905)
      prefix = '/' + prefix unless prefix[0] == '/'
      if ENV['KUBERNETES_SERVICE_HOST']
        hostname = "DEFAULT__USER__#{microservice_name}" unless hostname
        hostname = hostname.downcase.gsub("__", "-").gsub("_", "-")
        return "#{schema}://#{hostname}-service:#{port.to_i}#{prefix}"
      else
        hostname = 'openc3-operator' unless hostname
        return "#{schema}://#{hostname}:#{port.to_i}#{prefix}"
      end
    end

    # generate the auth object
    def _generate_auth
      if ENV['OPENC3_API_TOKEN'].nil? and ENV['OPENC3_API_USER'].nil?
        if ENV['OPENC3_API_PASSWORD'] || ENV['OPENC3_SERVICE_PASSWORD']
          return OpenC3::OpenC3Authentication.new()
        else
          return nil
        end
      else
        return OpenC3::OpenC3KeycloakAuthentication.new(ENV['OPENC3_KEYCLOAK_URL'])
      end
    end

    def _request(*method_params, **kw_params)
      kw_params[:scope] = $openc3_scope unless kw_params[:scope]
      @json_api.request(*method_params, **kw_params)
    end
  end
end

# Usage:
#
# In ScriptRunner:
# require 'cfdp_api'
# api = CfdpApi.new
# api.example_method
#
# Outside cluster - Open Source:
# require 'cfdp_api'
# $openc3_scope = 'DEFAULT'
# ENV['OPENC3_API_PASSWORD'] = 'password'
# api = CfdpApi.new(hostname: '127.0.0.1', port: 2900)
# api.example_method
#
# Outside cluster - Enterprise
# require 'cfdp_api'
# $openc3_scope = 'DEFAULT'
# ENV['OPENC3_KEYCLOAK_URL'] = '127.0.0.1:2900'
# ENV['OPENC3_API_USER'] = 'operator'
# ENV['OPENC3_API_PASSWORD'] = 'operator'
# api = CfdpApi.new(hostname: '127.0.0.1', port: 2900)
# api.example_method
#
class CfdpApi < OpenC3::JsonApi
  def put(scope: $openc3_scope)
    response = _request('get', '/', scope: scope)
    puts response.status
  end
end

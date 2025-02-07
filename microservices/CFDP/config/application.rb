require_relative "boot"

require "rails"
require "action_controller/railtie"

# Require the gems listed in Gemfile, including any gems
# you've limited to :test, :development, or :production.
Bundler.require(*Rails.groups)

module CfdpApi
  class Application < Rails::Application
    # Initialize configuration defaults for originally generated Rails version.
    config.load_defaults 7.0
    # Add to hosts to prevent "Blocked host" errors
    config.hosts.clear

    # Configuration for the application, engines, and railties goes here.
    #
    # These settings can be overridden in specific environments using the files
    # in config/environments, which are processed later.
    #
    # config.time_zone = "Central Time (US & Canada)"
    # config.eager_load_paths << Rails.root.join("extras")

    # Only loads a smaller set of middleware suitable for API only apps.
    # Middleware like session, flash, cookies can be added back manually.
    # Skip views, helpers and assets when generating a new resource.
    config.api_only = true

    # config.action_cable.disable_request_forgery_protection = true
    # config.action_cable.mount_path = '/script-api/cable'

    OpenC3::Logger.microservice_name = ENV['OPENC3_MICROSERVICE_NAME']

    require 'openc3/utilities/open_telemetry'
    OpenC3.setup_open_telemetry(ENV['OPENC3_MICROSERVICE_NAME'], true)
    if OpenC3.otel_enabled
      config.middleware.insert_before(
        0,
        OpenTelemetry::Instrumentation::Rack::Middlewares::TracerMiddleware
      )
    end
  end
end

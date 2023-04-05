Rails.application.routes.draw do
  if ENV['RAILS_ENV'] == 'test'
    prefix = "/cfdp" # Default from plugin.txt
  else
    prefix = "<%= cfdp_route_prefix %>"
  end
  prefix = prefix[1..-1] if prefix[0] == '/'
  scope prefix do
    post "/put" => "cfdp#put"
    post "/cancel" => "cfdp#cancel"
    post "/suspend" => "cfdp#suspend"
    post "/resume" => "cfdp#resume"
    post "/report" => "cfdp#report"
    get "/indications/:transaction_id" => "cfdp#indications"
    get "/indications" => "cfdp#indications"
  end
end

Rails.application.routes.draw do
  prefix = "<%= cfdp_route_prefix %>"
  prefix = prefix[1..-1] if prefix[0] == '/'
  scope prefix do
    post "/put" => "cfdp#put"
    get "/indications/:transaction_id" => "cfdp#indications"
    get "/indications" => "cfdp#indications"
  end
end

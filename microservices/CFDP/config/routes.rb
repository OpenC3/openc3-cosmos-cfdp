Rails.application.routes.draw do
  prefix = "<%= cfdp_route_prefix %>"
  prefix = prefix[1..-1] if prefix[0] == '/'
  scope prefix do
    get "/" => "cfdp#index"
  end
end

Rails.application.routes.draw do
  prefix = ENV['OPENC3_ROUTE_PREFIX'] || "/cfdp"
  prefix = prefix[1..-1] if prefix[0] == '/'
  scope prefix do
    post "/put" => "cfdp#put"
    post "/put_dir" => "cfdp#put_dir"
    post "/cancel" => "cfdp#cancel"
    post "/suspend" => "cfdp#suspend"
    post "/resume" => "cfdp#resume"
    post "/report" => "cfdp#report"
    post "/directorylisting" => "cfdp#directory_listing"
    get "/subscribe" => "cfdp#subscribe"
    get "/indications/:transaction_id" => "cfdp#indications"
    get "/indications" => "cfdp#indications"
    get "/transactions" => "cfdp#transactions"
  end
end

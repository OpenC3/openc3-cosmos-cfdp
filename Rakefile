PLUGIN_NAME = Dir['*.gemspec'][0].split('.')[0..-2].join('.')

task :require_version do
  unless ENV['VERSION']
    puts "VERSION is required: rake build VERSION=X.Y.Z"
    exit 1
  end
end

task :build => [:require_version] do
  # Build using sh built into Rake:
  # https://rubydoc.info/gems/rake/FileUtils#sh-instance_method
  sh('gem', 'build', PLUGIN_NAME)
  sh('openc3cli validate *.gem') do |ok, status|
    if !ok && status.exitstatus == 127 # command not found
      puts "Install the openc3 gem to validate! (gem install openc3)"
    end
  end
end

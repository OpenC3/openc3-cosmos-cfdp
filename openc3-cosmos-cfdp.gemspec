# encoding: ascii-8bit

# Copyright 2026 OpenC3, Inc.
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

# Create the overall gemspec
spec = Gem::Specification.new do |s|
  s.name = 'openc3-cosmos-cfdp'
  s.summary = 'CFDP'
  s.description = "This plugin provides COSMOS Support for CCSDS File Delivery Protocol (CFDP). It includes a COSMOS API for sending CFDP commands and an example COSMOS Target that can be used to receive files sent via CFDP."
  s.authors = ['Ryan Melton', 'Jason Thomas']
  s.email = ['support@openc3.com']
  s.homepage = 'https://github.com/OpenC3/openc3-cosmos-cfdp'

  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = '>= 3.0'

  if ENV['VERSION']
    s.version = ENV['VERSION'].dup
  else
    time = Time.now.strftime("%Y%m%d%H%M%S")
    s.version = '0.0.0' + ".#{time}"
  end
  s.license = 'Commercial'

  s.metadata = {
    "openc3_store_keywords" => "cfdp, ccsds, file, protocol",
    "openc3_cosmos_minimum_version" => "6.10.2"
  }

  s.require_paths = ['lib', 'microservices/CFDP/app/models']
  s.files = Dir.glob("{targets,lib,public,tools,microservices}/**/*") + %w(Rakefile LICENSE.md README.md plugin.txt)
end

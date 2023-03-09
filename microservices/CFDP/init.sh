#!/bin/sh
# set -x

# Fail on errors
set -e

bundle config set --local without 'development test'
bundle install --quiet
rails s -b 0.0.0.0 -p <%= cfdp_port %>

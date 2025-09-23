# encoding: ascii-8bit

# Copyright 2025 OpenC3, Inc.
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

class CfdpNullChecksum
  def add(offset, data)
    return 0
  end

  def checksum(file, full_checksum_needed)
    return 0
  end

  def check(file, other_checkum, full_checksum_needed)
    true
  end

  def self.json_create(object)
    return CfdpNullChecksum.new()
  end

  def as_json(_options = nil)
    return { "json_class" => self.class.to_s }
  end
end

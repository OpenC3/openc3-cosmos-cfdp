# encoding: ascii-8bit

# Copyright 2023 OpenC3, Inc.
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

require 'openc3/utilities/crc'

class CfdpCrcChecksum
  def initialize(poly, seed, xor, reflect)
    @crc = OpenC3::Crc32.new(poly, seed, xor, reflect)
    @checksum = 0
  end

  # Incremental not supported so add ignored
  def add(offset, data)
    return 0
  end

  # Uses file because incremental add is not supported
  def checksum(file, full_checksum_needed)
    file.rewind
    data = file.read
    @checksum = @crc.calc(data)
    return @checksum & 0xFFFFFFFF
  end

  def check(file, other_checksum, full_checksum_needed)
    checksum(file, full_checksum_needed) == other_checksum
  end
end
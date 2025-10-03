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
# The development of this software was funded in-whole or in-part by Sandia National Laboratories.
# See https://github.com/OpenC3/openc3-cosmos-cfdp/pull/12 for details

require 'rails_helper'
require 'tempfile'
require 'openc3/utilities/crc'

RSpec.describe CfdpCrcChecksum do
  describe "initialize" do
    it "initializes with default values" do
      checksum = CfdpCrcChecksum.new
      expect(checksum.instance_variable_get(:@checksum)).to eq(0)
    end

    it "initializes with custom values" do
      # These are the defaults from the OpenC3::Crc32 class
      poly = 0x04C11DB7
      seed = 0xFFFFFFFF
      xor = true
      reflect = true

      # Expect it to be called with these specific parameters
      expect(OpenC3::Crc32).to receive(:new).with(poly, seed, xor, reflect).and_return(@mock_crc32)

      checksum = CfdpCrcChecksum.new(poly, seed, xor, reflect)
      expect(checksum.crc).to eq(@mock_crc32)
    end
  end

  describe "add" do
    it "always returns 0 (not supported)" do
      checksum = CfdpCrcChecksum.new
      result = checksum.add(100, "test data")
      expect(result).to eq(0)
    end
  end

  describe "checksum" do
    it "calculates checksum from file" do
      # Create a tempfile with content
      file = Tempfile.new('crc_test')
      file.write("test data")
      file.rewind

      checksum = CfdpCrcChecksum.new
      result = checksum.checksum(file, false)

      # The result should be masked to 32 bits
      expect(result).to eq(0xd308aeb2)

      file.close
      file.unlink
    end
  end

  describe "check" do
    it "returns true when checksums match" do
      # Create a tempfile with content
      file = Tempfile.new('crc_test')
      file.write("test data")
      file.rewind

      checksum = CfdpCrcChecksum.new

      # First, mock the internal checksum call to return a specific value
      expect(checksum).to receive(:checksum).with(file, false).and_return(0x12345678)

      # Then check against the same value
      expect(checksum.check(file, 0x12345678, false)).to be true

      file.close
      file.unlink
    end

    it "returns false when checksums don't match" do
      # Create a tempfile with content
      file = Tempfile.new('crc_test')
      file.write("test data")
      file.rewind

      checksum = CfdpCrcChecksum.new

      # First, mock the internal checksum call to return a specific value
      expect(checksum).to receive(:checksum).with(file, false).and_return(0x12345678)

      # Then check against a different value
      expect(checksum.check(file, 0x87654321, false)).to be false

      file.close
      file.unlink
    end

    it "handles an empty file" do
      # Create an empty tempfile
      file = Tempfile.new('crc_test')
      file.rewind

      checksum = CfdpCrcChecksum.new

      # For an empty file, mock the CRC to return a specific value
      expect(checksum).to receive(:checksum).with(file, false).and_return(0)

      # The check should pass when comparing with the same value
      expect(checksum.check(file, 0, false)).to be true

      file.close
      file.unlink
    end
  end

  describe "json" do
    it "round trips the json" do
      checksum = CfdpCrcChecksum.new(0xDEADBEEF, 0x12345678, false, false)
      json = JSON.generate(checksum.as_json)
      newcheck = JSON.parse(json, create_additions: true)
      expect(newcheck).to be_a(CfdpCrcChecksum)
      expect(newcheck.crc.poly).to eq(0xDEADBEEF)
      expect(newcheck.crc.seed).to eq(0x12345678)
      expect(newcheck.crc.xor).to be false
      expect(newcheck.crc.reflect).to be false
    end
  end
end

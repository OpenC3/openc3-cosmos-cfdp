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

require 'rails_helper'
require 'tempfile'

RSpec.describe CfdpNullChecksum do
  describe "initialize" do
    it "creates an instance" do
      checksum = CfdpNullChecksum.new
      expect(checksum).to be_a(CfdpNullChecksum)
    end
  end

  describe "add" do
    it "always returns 0 regardless of input" do
      checksum = CfdpNullChecksum.new

      expect(checksum.add(0, "")).to eq(0)
      expect(checksum.add(100, "test data")).to eq(0)
      expect(checksum.add(999, "large amount of data" * 100)).to eq(0)
    end
  end

  describe "checksum" do
    it "always returns 0 regardless of file content" do
      checksum = CfdpNullChecksum.new

      # Test with content file
      file = Tempfile.new('null_checksum_test')
      file.write("test data")
      file.rewind

      expect(checksum.checksum(file, false)).to eq(0)
      expect(checksum.checksum(file, true)).to eq(0)

      file.close
      file.unlink
    end

    it "returns 0 for empty file" do
      checksum = CfdpNullChecksum.new

      # Test with empty file
      file = Tempfile.new('null_checksum_test')
      file.rewind

      expect(checksum.checksum(file, false)).to eq(0)
      expect(checksum.checksum(file, true)).to eq(0)

      file.close
      file.unlink
    end
  end

  describe "check" do
    it "always returns true regardless of input" do
      checksum = CfdpNullChecksum.new

      # Create a tempfile with content
      file = Tempfile.new('null_checksum_test')
      file.write("test data")
      file.rewind

      # Should return true for any checksum value
      expect(checksum.check(file, 0, false)).to be true
      expect(checksum.check(file, 0x12345678, false)).to be true
      expect(checksum.check(file, 0xFFFFFFFF, true)).to be true
      expect(checksum.check(file, -1, false)).to be true

      file.close
      file.unlink
    end

    it "returns true for empty file with any checksum" do
      checksum = CfdpNullChecksum.new

      # Test with empty file
      file = Tempfile.new('null_checksum_test')
      file.rewind

      expect(checksum.check(file, 0, false)).to be true
      expect(checksum.check(file, 0x87654321, true)).to be true

      file.close
      file.unlink
    end
  end

  describe "json" do
    it "round trips the json" do
      checksum = CfdpNullChecksum.new
      json = JSON.generate(checksum.as_json)
      newcheck = JSON.parse(json, create_additions: true)
      expect(newcheck).to be_a(CfdpNullChecksum)
    end
  end
end
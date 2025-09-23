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

RSpec.describe CfdpChecksum do
  describe "initialize" do
    it "initializes with default checksum of 0" do
      checksum = CfdpChecksum.new
      expect(checksum.instance_variable_get(:@checksum)).to eq(0)
    end

    it "initializes with custom checksum value" do
      checksum = CfdpChecksum.new(12345)
      expect(checksum.instance_variable_get(:@checksum)).to eq(12345)
    end
  end

  describe "add" do
    it "adds data starting at offset 0 (aligned)" do
      checksum = CfdpChecksum.new
      # 4-byte aligned data: "TEST" = [0x54455354]
      result = checksum.add(0, "TEST")
      expected = 0x54455354
      expect(result).to eq(expected)
    end

    it "adds data with front padding for unaligned offset" do
      checksum = CfdpChecksum.new
      # Offset 1 means 1 byte front padding
      # "\x00TEST" with 3 bytes end padding = "\x00TEST\x00\x00\x00"
      # This becomes [0x00544553, 0x54000000]
      result = checksum.add(1, "TEST")
      expected = 0x00544553 + 0x54000000
      expect(result).to eq(expected)
    end

    it "adds data with end padding for unaligned length" do
      checksum = CfdpChecksum.new
      # "ABC" with 1 byte end padding = "ABC\x00"
      # This becomes [0x41424300]
      result = checksum.add(0, "ABC")
      expected = 0x41424300
      expect(result).to eq(expected)
    end

    it "accumulates multiple add operations" do
      checksum = CfdpChecksum.new
      result1 = checksum.add(0, "TEST")
      result2 = checksum.add(0, "DATA")

      expected1 = 0x54455354
      expected2 = expected1 + 0x44415441

      expect(result1).to eq(expected1)
      expect(result2).to eq(expected2)
    end

    it "handles empty data" do
      checksum = CfdpChecksum.new
      result = checksum.add(0, "")
      expect(result).to eq(0)
    end

    it "handles single byte data" do
      checksum = CfdpChecksum.new
      # "A" with 3 bytes padding = "A\x00\x00\x00"
      # This becomes [0x41000000]
      result = checksum.add(0, "A")
      expected = 0x41000000
      expect(result).to eq(expected)
    end
  end

  describe "checksum" do
    it "returns current checksum when full_checksum_needed is false" do
      checksum = CfdpChecksum.new(12345)
      file = Tempfile.new('checksum_test')
      file.write("test data")
      file.rewind

      result = checksum.checksum(file, false)
      expect(result).to eq(12345)

      file.close
      file.unlink
    end

    it "calculates full checksum when full_checksum_needed is true" do
      checksum = CfdpChecksum.new(12345) # Initial value should be reset
      file = Tempfile.new('checksum_test')
      file.write("TEST") # 4-byte aligned
      file.rewind

      result = checksum.checksum(file, true)
      expected = 0x54455354 & 0xFFFFFFFF
      expect(result).to eq(expected)

      file.close
      file.unlink
    end

    it "masks result to 32 bits" do
      checksum = CfdpChecksum.new(0xFFFFFFFFFFFFFFFF) # 64-bit value
      file = Tempfile.new('checksum_test')
      file.write("")
      file.rewind

      result = checksum.checksum(file, false)
      expect(result).to eq(0xFFFFFFFF) # Masked to 32 bits

      file.close
      file.unlink
    end

    it "rewinds and reads entire file when full_checksum_needed is true" do
      checksum = CfdpChecksum.new
      file = Tempfile.new('checksum_test')
      file.write("TESTDATA")
      file.seek(4) # Move file position

      result = checksum.checksum(file, true)
      # Should read entire file "TESTDATA", not just from position 4
      expected = (0x54455354 + 0x44415441) & 0xFFFFFFFF
      expect(result).to eq(expected)

      file.close
      file.unlink
    end
  end

  describe "check" do
    it "returns true when checksums match" do
      checksum = CfdpChecksum.new
      file = Tempfile.new('checksum_test')
      file.write("TEST")
      file.rewind

      expected_checksum = 0x54455354
      expect(checksum.check(file, expected_checksum, true)).to be true

      file.close
      file.unlink
    end

    it "returns false when checksums don't match" do
      checksum = CfdpChecksum.new
      file = Tempfile.new('checksum_test')
      file.write("TEST")
      file.rewind

      wrong_checksum = 0x12345678
      expect(checksum.check(file, wrong_checksum, true)).to be false

      file.close
      file.unlink
    end

    it "uses current checksum when full_checksum_needed is false" do
      checksum = CfdpChecksum.new(0x12345678)
      file = Tempfile.new('checksum_test')
      file.write("anything") # Content doesn't matter
      file.rewind

      expect(checksum.check(file, 0x12345678, false)).to be true
      expect(checksum.check(file, 0x87654321, false)).to be false

      file.close
      file.unlink
    end
  end

  describe "json" do
    it "round trips the json" do
      checksum = CfdpChecksum.new(0xDEADBEEF)
      json = JSON.generate(checksum.as_json)
      newcheck = JSON.parse(json, create_additions: true)
      expect(newcheck).to be_a(CfdpChecksum)
      expect(newcheck.instance_variable_get(:@checksum)).to eq(0xDEADBEEF)
    end
  end

  describe "padding behavior" do
    it "handles various offset alignments" do
      checksum = CfdpChecksum.new

      # Test all possible offset alignments
      result0 = checksum.add(0, "A") # offset % 4 = 0, no front padding
      checksum = CfdpChecksum.new
      result1 = checksum.add(1, "A") # offset % 4 = 1, 1 byte front padding
      checksum = CfdpChecksum.new
      result2 = checksum.add(2, "A") # offset % 4 = 2, 2 bytes front padding
      checksum = CfdpChecksum.new
      result3 = checksum.add(3, "A") # offset % 4 = 3, 3 bytes front padding

      expect(result0).to eq(0x41000000) # "A\x00\x00\x00"
      expect(result1).to eq(0x00410000) # "\x00A\x00\x00"
      expect(result2).to eq(0x00004100) # "\x00\x00A\x00"
      expect(result3).to eq(0x00000041) # "\x00\x00\x00A"
    end

    it "handles data lengths requiring different end padding" do
      checksum = CfdpChecksum.new

      # 1 byte: needs 3 bytes end padding
      result1 = checksum.add(0, "A")
      expect(result1).to eq(0x41000000)

      checksum = CfdpChecksum.new
      # 2 bytes: needs 2 bytes end padding
      result2 = checksum.add(0, "AB")
      expect(result2).to eq(0x41420000)

      checksum = CfdpChecksum.new
      # 3 bytes: needs 1 byte end padding
      result3 = checksum.add(0, "ABC")
      expect(result3).to eq(0x41424300)

      checksum = CfdpChecksum.new
      # 4 bytes: no end padding needed
      result4 = checksum.add(0, "ABCD")
      expect(result4).to eq(0x41424344)
    end
  end
end

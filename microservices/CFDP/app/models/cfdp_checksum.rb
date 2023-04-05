class CfdpChecksum
  def initialize
    @checksum = 0
  end

  def add(offset, data)
    front_pad_bytes = offset % 4
    if front_pad_bytes != 0
      data = ("\x00" * front_pad_bytes) << data
    end
    end_pad_bytes = 4 - data.length % 4
    if end_pad_bytes != 4
      data = data + ("\x00" * end_pad_bytes)
    end
    values = data.unpack('N*')
    values.each do |value|
      @checksum += value
    end
    return @checksum
  end

  # Expected to be calculated as we go using add, so file unused
  def checksum(file, full_checksum_needed)
    if full_checksum_needed
      file.rewind
      data = file.read
      @checksum = 0
      add(0, data)
    end
    return @checksum & 0xFFFFFFFF
  end

  def check(file, other_checksum, full_checksum_needed)
    checksum(file, full_checksum_needed) == other_checksum
  end
end
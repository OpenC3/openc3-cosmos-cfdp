def CfdpNullChecksum
  def add(offset, data)
    return 0
  end

  def checksum(file, full_checksum_needed)
    return 0
  end

  def check(file, other_checkum, full_checksum_needed)
    true
  end
end
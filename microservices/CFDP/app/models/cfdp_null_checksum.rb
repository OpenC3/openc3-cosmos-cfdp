def CfdpNullChecksum
  def add(offset, data)
    return 0
  end

  def checksum(file)
    return 0
  end

  def check(file, other_checkum)
    true
  end
end
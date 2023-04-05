require 'openc3/utilities/crc'

class CrcChecksum
  def initialize(poly, seed, xor, reflect)
    @crc = OpenC3::Crc32.new(poly, seed, xor, reflect)
    @checksum = 0
  end

  # Incremental not supported so add ignored
  def add(offset, data)
    return 0
  end

  # Uses file because incremental add is not supported
  def checksum(file)
    file.rewind
    data = file.read
    @checksum = @crc.calc(data)
    return @checksum & 0xFFFFFFFF
  end

  def check(file, other_checksum)
    checksum(file) == other_checksum
  end
end
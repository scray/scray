package scray.hdfs.index.format

trait HasByteRepresentation {
  def getByteRepresentation: Array[Byte]
}
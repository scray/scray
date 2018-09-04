package scray.hdfs.io.write

import scray.hdfs.io.write.WriteResultType

case class Failure(val exception: Exception) extends WriteResult {
  override def getWriteResultType(): WriteResultType = {
    WriteResultType.FAILURE
  }
}
package scray.hdfs.io.write

import scray.hdfs.io.write.WriteResultType

case class Success() extends WriteResult {
  override def getWriteResultType(): WriteResultType = {
    WriteResultType.SUCCESS
  }
}
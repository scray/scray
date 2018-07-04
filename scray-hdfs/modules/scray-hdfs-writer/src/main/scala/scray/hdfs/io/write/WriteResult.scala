package scray.hdfs.io.write

import java.util.concurrent.Future

object WriteResultType extends Enumeration {
  type WriteResultType = Value
  val SUCCESS, FAILURE = Value
}

trait WriteResult {
  val resultType: WriteResultType.Value
}
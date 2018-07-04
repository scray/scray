package scray.hdfs.io.write

case class Failure() extends WriteResult {
  override val resultType = WriteResultType.FAILURE
  
}
package scray.hdfs.write

case class Failure() extends WriteResult {
  override val resultType = WriteResultType.FAILURE
  
}
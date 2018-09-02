package scray.hdfs.io.write

case class Failure(val exception: Exception) extends WriteResult {
  override val resultType = WriteResultType.FAILURE
  
}
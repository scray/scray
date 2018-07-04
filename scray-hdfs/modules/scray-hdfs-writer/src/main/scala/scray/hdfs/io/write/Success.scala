package scray.hdfs.io.write

case class Success() extends WriteResult {
  override val resultType = WriteResultType.SUCCESS
}
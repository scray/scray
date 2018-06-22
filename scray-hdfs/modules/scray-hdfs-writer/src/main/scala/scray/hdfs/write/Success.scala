package scray.hdfs.write

case class Success() extends WriteResult {
  override val resultType = WriteResultType.SUCCESS
}
package scray.sync.api

trait VersionedDataApi {
  def getLatestVersion(dataSource: String, mergeKey: String): Option[VersionedData]
  def updateVersion(dataSource: String, mergeKey: String, version: Long, data: String)
  def flush
}
package scray.sync.api

trait VersionedDataApi {
  def getLatestVersion(dataSource: String, mergeKey: String): VersionedData
  def writeNextVersion(dataSource: String, mergeKey: String, version: Long, data: String)
}
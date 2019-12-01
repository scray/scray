package scray.sync.api

class VersionedData(
  val dataSource: String,  // Defines where the data come from. E.g. from a batch or streaming job
  val mergeKey: String,    // Describes a attribute to merge two dataSources
  val version: Long,       // Version of this data. E.g. time stamp
  val data: String         // String representation of the data to store
) {
  
  def getDataAs[T](f: String => T): T = {
    f(data)
  }
  
  def getVersionKey: Int = {
    VersionedData.createVersionKey(dataSource, mergeKey)
  }
  
  override def toString: String = {
    s"""{"dataSource: "${dataSource}", "mergeKey": "${mergeKey}", "version": ${version}, "data": "${data}"}"""
  }
}

object VersionedData {
  def createVersionKey(dataSource: String, mergeKey: String): Int = {
    dataSource.hashCode() * 31 + mergeKey.hashCode() * 31
  }
}
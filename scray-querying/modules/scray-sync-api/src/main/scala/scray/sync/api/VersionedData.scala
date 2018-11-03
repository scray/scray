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
}
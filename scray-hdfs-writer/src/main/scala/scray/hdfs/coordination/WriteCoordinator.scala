package scray.hdfs.coordination

trait WriteCoordinator {
  /**
   * Destination where new data should be written.
   */
  def getWriteDestination(queryspace: String): Option[String]
}
package scray.querying.sync.types

case class BatchMetadata(
    batchStartTime: Option[Long] = None,
    batchEndTime: Option[Long] = None)
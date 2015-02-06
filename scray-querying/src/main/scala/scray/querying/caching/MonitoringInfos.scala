package scray.querying.caching

case class MonitoringInfos(sizeGB: Double,
                           entries: Long,
                           currentSize: Long,
                           freeSize: Long)
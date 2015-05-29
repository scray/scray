package scray.storm.scheme

/**
 * Abstract journal entry which reflects a reference into the original data set
 */
case class ScrayKafkaJournalEntry[T](tableName: String, reference: T, time: Long, delete: Boolean)
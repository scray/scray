package scray.querying.source.indexing

import java.util.TimeZone
import scray.querying.description.Column

/**
 * all indexes that are allowed must have a configuration
 * that inherits from this
 */
sealed trait IndexConfig extends Serializable

/**
 * configuration which must be passed to TimeIndexSource
 */
case class TimeIndexConfig(
    timeReferenceCol: Column, 
    indexRowColumnYear: Column,
    indexColumnMs: Column, 
    indexReferencesColumn: Column,
    timeZone: TimeZone = TimeZone.getTimeZone("UTC"),
    minimumYear: Int = 2014,
    maxLimit: Option[Long] = Some(5000)) extends IndexConfig

/**
 * simple in-stream index
 */
case class SimpleHashJoinConfig() extends IndexConfig
package scray.querying.source.indexing

import java.util.TimeZone
import scray.querying.description.Column

/**
 * all indexes that are allowed must have a configuration
 * that inherits from this
 */
sealed trait IndexConfig extends Serializable

/**
 * configuration for TimeIndexSource
 */
case class TimeIndexConfig(
    // column in the lookup-table that can be queried
    timeReferenceCol: Column,
    // column in the index-table that reflects the year for the time
    indexRowColumnYear: Column,
    // column in the index-table that contains the time information in ms
    indexColumnMs: Column,
    // column in the index-table that contains the references into the lookup-source
    indexReferencesColumn: Column,
    // time zone of index, only relevant for turns of the year
    timeZone: TimeZone = TimeZone.getTimeZone("UTC"), 
    // minimum year of data that is indexed
    minimumYear: Int = 2014,
    // query limit
    maxLimit: Option[Long] = Some(5000)) extends IndexConfig

/**
 * configuration for WildcardIndexSource
 */
case class WildcardIndexConfig(
    // column in the lookup-table that can be queried
    referenceCol: Column,
    // first letters to be indexed
    indexRowNameCol: Column,
    // UTF8-Column for the name which is indexed
    indexNameColumn: Column,
    // column in the index-table that contains the references into the lookup-source
    indexReferencesColumn: Column,
    // the number of characters in the prefix
    numberOfPrefixCharacters: Int,
    // if the index is lower case, i.e. enables
    lowerCaseIndex: Boolean,
    // if the prefix is present in the data
    prefixPresent: Boolean,
    // query limit
    maxLimit: Option[Long] = Some(5000)) extends IndexConfig    

/**
 * simple in-stream index
 */
case class SimpleHashJoinConfig() extends IndexConfig
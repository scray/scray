package scray.querying.caching

import scray.querying.description.EmptyRow

/**
 * marker rows denoting the end of a cache
 * serving action
 */
sealed trait CacheServeMarkerRow extends EmptyRow

/**
 * marker row that denotes cache ending stating 
 * that the cache served all rows 
 */
class CompleteCacheServeMarkerRow extends CacheServeMarkerRow

/**
 * marker row that denotes cache ending stating
 * that the cache served some rows, but there might be
 * more in the database; this is the default, if the 
 * all rows are returned and the cache is exhausted
 */
class IncompleteCacheServeMarkerRow extends CacheServeMarkerRow
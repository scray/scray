package scray.core.service

import scray.querying.description.Row
import com.twitter.bijection.Bijection
import com.twitter.util.Duration
package object spools {

  // time-to-life of memcached query results 
  final val DEFAULT_TTL : Duration = Duration.fromSeconds(180)

  // default number of rows per page
  final val DEFAULT_PAGESIZE : Int = 50

}
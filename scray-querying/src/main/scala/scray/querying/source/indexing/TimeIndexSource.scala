// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.querying.source.indexing

import com.twitter.util.Time
import com.typesafe.scalalogging.slf4j.LazyLogging

import java.util.{Calendar, GregorianCalendar, TimeZone}

import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import scray.querying.description.{Column, ColumnGrouping, ColumnOrdering, QueryRange, Row, TableIdentifier}
import scray.querying.description.internal.{Bound, Domain, QueryDomainRangeException, RangeValueDomain, SingleValueDomain}
import scray.querying.description.internal.CombinedIndexColumnMissingException
import scray.querying.description.internal.ComposedMultivalueDomain
import scray.querying.queries.DomainQuery
import scray.querying.source.{AbstractHashJoinSource, KeyValueSource, LazySource}

/**
 * creates an indexed-source with a hashed-join reference on a time column.
 * 
 * Format of this hand-made index will be:
 * TIME[a], TIME[ms], Set[ref]
 */
class TimeIndexSource[Q <: DomainQuery, M, R, V](
    timeIndexConfig: TimeIndexConfig,
    indexsource: LazySource[Q],
    lookupSource: KeyValueSource[R, V],
    lookupSourceTable: TableIdentifier,
    lookupkeymapper: Option[M => R] = None,
    sequencedmapper: Option[Int] = None,
    combinedIndexColumns: Set[Column] = Set(),
    useranges: Boolean = false)(implicit tag: ClassTag[M]) 
    extends AbstractHashJoinSource[Q, M, R, V](indexsource, lookupSource, lookupSourceTable, lookupkeymapper, combinedIndexColumns, sequencedmapper)
    with LazyLogging {

  /**
   * return the year from a time using GregorianCalendar and the given TimeZone
   */
  @inline private def getYearFromTime(time: Time): Int = {
    val greg = new GregorianCalendar
    greg.setTimeZone(timeIndexConfig.timeZone)
    greg.setTime(time.toDate)
    greg.get(Calendar.YEAR)
  }
  
  /**
   * creates a domain query that matches the provided domains and trys to reflect
   * the original query options
   */
  @inline private def createDomainQuery(query: Q, domains: List[Domain[_]]): Q = {
    val resultColumns = List(timeIndexConfig.indexRowColumnYear,
        timeIndexConfig.indexColumnMs, timeIndexConfig.indexReferencesColumn)
    val range = if(useranges) {
        query.getQueryRange.map { qrange =>
          val skipLines = qrange.skip.getOrElse(0L)
          QueryRange(None, qrange.limit.map(_ + skipLines).orElse(timeIndexConfig.maxLimit.map(_ + skipLines)))
        }
      } else {
        None
      }
    DomainQuery(query.getQueryID, query.getQueryspace, query.querySpaceVersion, resultColumns, timeIndexConfig.indexRowColumnYear.table,
        domains, Some(ColumnGrouping(timeIndexConfig.indexColumnMs)),
        Some(ColumnOrdering[Long](timeIndexConfig.indexColumnMs, 
                query.getOrdering.filter(_.descending).isDefined)), range).asInstanceOf[Q]
  }
  
  override protected def transformIndexQuery(query: Q): Set[Q] = {
    val combinedIndexDomains = getCombinedIndexColumns(query, timeIndexConfig.indexRowColumnYear.table)
    val optDomain = query.getWhereAST.find(domain => domain.column == timeIndexConfig.timeReferenceCol)
    optDomain.map(_ match {
      case cmd: ComposedMultivalueDomain[_] => ??? // currently unused domain type
      case svd: SingleValueDomain[_] => {
        val time = Time.fromMilliseconds(svd.value.asInstanceOf[Long])
        val yearDomain = new SingleValueDomain(timeIndexConfig.indexRowColumnYear, getYearFromTime(time))
        val timeDomain = new SingleValueDomain(timeIndexConfig.indexColumnMs, time.inMilliseconds)
        Set(createDomainQuery(query, List(yearDomain, timeDomain) ++ combinedIndexDomains))
      }
      case rvd: RangeValueDomain[_] => {
        val years = Buffer[Int]()
        val transformedRangeValueDomain = rvd.lowerBound match {
          case Some(start) => {
            val startValue = start.value.asInstanceOf[Long]
            val startYear = getYearFromTime(Time.fromMilliseconds(startValue))
            val realStartYear = if(startYear < timeIndexConfig.minimumYear) {
              timeIndexConfig.minimumYear
            } else {
              startYear
            }
            val startBound = Some(Bound[Long](start.inclusive, startValue))
            rvd.upperBound match {
              case Some(end) => 
                val endValue = end.value.asInstanceOf[Long]
                val endBound = Some(Bound[Long](end.inclusive, endValue))
                // add all years in between; if endValue < realStartYear => return nothing
                if(endValue < realStartYear) {
                  years += 9999 // obviously queries will return no data for this 
                } else {
                  years ++= realStartYear.to(getYearFromTime(Time.fromMilliseconds(endValue)))
                }
                Some(new RangeValueDomain[Long](timeIndexConfig.indexColumnMs, startBound, endBound))
              case None => {
                // add years from back then up to now
                years ++= realStartYear.to(getYearFromTime(Time.now))
                Some(new RangeValueDomain[Long](timeIndexConfig.indexColumnMs, startBound, None))
              }
            }
          }
          case None => rvd.upperBound match {
            case Some(end) => {
              // warning: query years all up to end!
              val endValue = end.value.asInstanceOf[Long]
              val endBound = Some(Bound[Long](end.inclusive, endValue))
              years ++= timeIndexConfig.minimumYear.to(getYearFromTime(Time.now))
              Some(new RangeValueDomain[Long](timeIndexConfig.indexColumnMs, None, endBound))
            }
            case None => throw new QueryDomainRangeException(timeIndexConfig.timeReferenceCol, query)
          }
        }
        years.map { year =>
          val yearDomain = new SingleValueDomain(timeIndexConfig.indexRowColumnYear, year)
          createDomainQuery(query, transformedRangeValueDomain.map(List(_, yearDomain)).getOrElse(List(yearDomain) ++ combinedIndexDomains))
        }.toSet
      } 
    }).getOrElse {
      // warning: query years all up to now!
      timeIndexConfig.minimumYear.to(getYearFromTime(Time.now)).map(year =>
          createDomainQuery(query, List(new SingleValueDomain(timeIndexConfig.indexRowColumnYear, year)) ++ combinedIndexDomains)).toSet
    }
  }
  
  override protected def getJoinablesFromIndexSource(index: Row): Array[M] = {
    index.getColumnValue[M](timeIndexConfig.indexReferencesColumn) match {
      case Some(refs) => refs match {
        case travs: TraversableOnce[M] => travs.asInstanceOf[TraversableOnce[M]].toArray
        case travs: M => Array[M](travs)
      }
      case None => Array[M]()
    }
  }

  override protected def isOrderedAccordingToOrignalOrdering(transformedQuery: Q, ordering: ColumnOrdering[_]): Boolean =
    ordering.column == timeIndexConfig.timeReferenceCol
  
  /**
   * since this is a true index only  
   */
  override def getColumns: List[Column] = lookupSource.getColumns
}

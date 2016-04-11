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
import java.util.{Calendar, GregorianCalendar, TimeZone}
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scray.querying.description.{Column, ColumnGrouping, ColumnOrdering, QueryRange, Row, TableIdentifier}
import scray.querying.description.internal.{Bound, Domain, QueryDomainRangeException, RangeValueDomain, SingleValueDomain}
import scray.querying.queries.DomainQuery
import scray.querying.source.{AbstractRangeSetHashJoinSource, KeyValueSource, LazySource}
import scray.querying.description.internal.WildcardIndexRangeException
import scala.collection.mutable.HashSet
import com.twitter.concurrent.Spool
import com.twitter.util.Await
import scray.querying.description.internal.ComposedMultivalueDomain
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * creates an indexed-source with a wildcard-join reference on a UTF8-column.
 * 
 * Format of this hand-made index will be:
 * UTF8[first 3 letters], UTF8[name to be indexed], Set[ ref ]
 */
class WildcardIndexSource[Q <: DomainQuery, M, R, V](
    wildcardIndexConfig: WildcardIndexConfig,
    indexsource: LazySource[Q],
    lookupSource: KeyValueSource[R, V],
    lookupSourceTable: TableIdentifier,
    lookupkeymapper: Option[M => R] = None)(implicit tag: ClassTag[M]) 
    extends AbstractRangeSetHashJoinSource[Q, M, R, V](indexsource, lookupSource, lookupSourceTable, lookupkeymapper, wildcardIndexConfig.maxLimit)
    with LazyLogging {

  /**
   * the name of the column in lookupsource that will be the primary 
   * key that is used as reference indexed
   */
  @inline protected def getReferenceLookupSourceColumn: Column = wildcardIndexConfig.referenceCol

  /**
   * the column in the index that contains a set of references into lookupsource
   */
  @inline protected def getReferencesIndexColumn: Column = wildcardIndexConfig.indexReferencesColumn
  
  /**
   * the column in the idnex that contains the value of the index data 
   */
  @inline protected def getValueIndexColumn: Column = wildcardIndexConfig.indexNameColumn
  
  /**
   * a primary column in the index used as a criteria shrinking 
   * the number of possible results
   */
  @inline protected def getPrefixIndexColumn: Column = wildcardIndexConfig.indexRowNameCol
  
  /**
   * return the first letters of the wildcard index
   */
  @inline private def getFirstLetters(name: String): String =
    name.substring(0, wildcardIndexConfig.numberOfPrefixCharacters)
  
  /**
   * returns a name that conforms 
   */
  @inline private def getReducedName(name: String): String = wildcardIndexConfig.prefixPresent match {
    case true => name
    case false => name.substring(wildcardIndexConfig.numberOfPrefixCharacters)
  }
  
  /**
   * normalize the name for this index, i.e. if it should work for 
   */
  @inline private def getNormalizedName(name: String): String = wildcardIndexConfig.lowerCaseIndex match {
    case true => name.toLowerCase()
    case _ => name
  }
  
  /**
   * fetch prefixes, which lie in the provided range
   */
  private def fetchPrefixes(query: Q, start: Option[String], end: Option[String]): Buffer[SingleValueDomain[String]] = {
    val result = Buffer[SingleValueDomain[String]]()
    def bufferSpool(spool: Spool[Row]): Unit = {
      if(!spool.isEmpty) {
        spool.head.getColumnValue[String](getValueIndexColumn).map(
            result += new SingleValueDomain(wildcardIndexConfig.indexRowNameCol, _))
        bufferSpool(Await.result(spool.tail))
      }
    }
    val empty = new SingleValueDomain(wildcardIndexConfig.indexNameColumn, "")
    val range = new RangeValueDomain[String](getValueIndexColumn,
        start.map(Bound[String](true, _)), end.map(Bound[String](true, _)))
    val dq = createDomainQuery[String](query, List(empty, range))
    bufferSpool(Await.result(indexsource.request(dq)))
    result
  }

  /**
   * Transform the original query into a set of queries, which 
   * Because it can happen that ranges have been defined, but the index doesn't include the
   * prefix, we need to 
   * This 
   */
  override protected def transformIndexQuery(query: Q): Set[Q] = {
    val optDomain = query.getWhereAST.find(domain => domain.column == wildcardIndexConfig.referenceCol)
    optDomain.map(_ match {
    case cmd: ComposedMultivalueDomain[_] => ??? // currently unused domain type  
    case svd: SingleValueDomain[_] => {
        val name = getNormalizedName(svd.value.asInstanceOf[String])
        val rowDomain = new SingleValueDomain(wildcardIndexConfig.indexRowNameCol, getFirstLetters(name))
        val nameDomain = new SingleValueDomain(getValueIndexColumn, getReducedName(name))
        Set(createDomainQuery[String](query, List(rowDomain, nameDomain)))
      }
      case rvd: RangeValueDomain[_] => {
        val prefixes = Buffer[List[Domain[String]]]()
        rvd.lowerBound match {
          case Some(start) => {
            val startValue = getNormalizedName(start.value.asInstanceOf[String])
            val startPrefix = getFirstLetters(startValue)
            val startBound = Some(Bound[String](start.inclusive, getReducedName(startValue)))
            rvd.upperBound match {
              case Some(end) =>
                val endValue = getNormalizedName(end.value.asInstanceOf[String])
                val endPrefix = getFirstLetters(endValue)
                val endBound = Some(Bound[String](end.inclusive, getReducedName(endValue)))
                if(endPrefix != startPrefix) {
                  // need to fetch range of prefixes
                  prefixes ++= fetchPrefixes(query, Some(startPrefix), Some(endPrefix)).map(List(_))
                  prefixes.size match {
                    case 1 => prefixes(0) = List(prefixes(0)(0), new RangeValueDomain[String](getValueIndexColumn, startBound, endBound))
                    case x if x > 1 => 
                      prefixes(0) = List(prefixes(0)(0), new RangeValueDomain[String](getValueIndexColumn, startBound, None))
                      prefixes(prefixes.size - 1) = List(prefixes(prefixes.size - 1)(0), new RangeValueDomain[String](getValueIndexColumn, None, endBound))
                    case _ =>
                  }
                } else {
                  prefixes += List(
                      new SingleValueDomain(wildcardIndexConfig.indexRowNameCol, startPrefix),
                      new RangeValueDomain[String](getValueIndexColumn, startBound, endBound)
                  )
                }
              case None => {
                // need to fetch range of prefixes
                prefixes ++= fetchPrefixes(query, Some(startPrefix), None).map(List(_))
                if(prefixes.size > 0) {
                  prefixes(0) = List(prefixes(0)(0), new RangeValueDomain[String](getValueIndexColumn, startBound, None))
                }                
              }
            }
          }
          case None => rvd.upperBound match {
            case Some(end) => {
              val endValue = getNormalizedName(end.value.asInstanceOf[String])
              val endPrefix = getFirstLetters(endValue)
              val endBound = Some(Bound[String](end.inclusive, getReducedName(endValue)))
              prefixes ++= fetchPrefixes(query, None, Some(endPrefix)).map(List(_))
              if(prefixes.size > 0) {
                prefixes(prefixes.size - 1) = List(prefixes(prefixes.size - 1)(0), 
                    new RangeValueDomain[String](getValueIndexColumn, None, endBound))
              }
            }
            case None => throw new QueryDomainRangeException(wildcardIndexConfig.referenceCol, query)
          }
        }
        prefixes.map(createDomainQuery[String](query, _)).toSet
      } 
    }).getOrElse {
      // need to fetch all prefixes
      fetchPrefixes(query, None, None).map(prefix => createDomainQuery[String](query, List(prefix))).toSet
    }
  }
}

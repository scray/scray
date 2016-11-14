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
package scray.cassandra.extractors

import scray.querying.queries.DomainQuery
import scray.querying.description.internal.{Domain, RangeValueDomain, SingleValueDomain}
import scray.querying.description.Column
import scray.querying.Registry
import com.typesafe.scalalogging.slf4j.LazyLogging
import DomainToCQLQueryMapping.{AND_LITERAL, EMPTY_LITERAL, ORDER_LITERAL, DESC_LITERAL, LIMIT_LITERAL}
import scray.cassandra.CassandraQueryableSource
import scray.querying.description.TableIdentifier

/**
 * performs mapping of DomainQueries to valid Cassandra CQL queries,
 * containing all the possible predicates for a single table as defined
 * in the domains
 */
class DomainToCQLQueryMapping[Q <: DomainQuery, S <: CassandraQueryableSource[Q]] extends LazyLogging {

  /**
   * add an ordering specification, if needed
   */
  def addOrderMapping(orgQuery: String, store: S, query: Q,
      storeTableNickName: Option[String]): String = query.getOrdering.map { order =>
    // check for the right-most clustering column. We order for this, only.
    val clusterCols = store.getClusteringKeyColumns
    val domains = clusterColumnDomains(clusterCols, store, query, storeTableNickName)
    if(order.column == domains.takeRight(1)(0).column) {
      val desc = if(order.descending) {
        DESC_LITERAL
      } else {
        EMPTY_LITERAL
      }
      s"${ORDER_LITERAL}${desc}"
    } else {
      EMPTY_LITERAL
    }
  }.getOrElse(orgQuery)

  @inline private def removeQuotes(query: String): String = query.filterNot(c => c == '"' || c == ';' || c == ''')
  
  @inline private def decideWhere(where: String): String = if(!where.isEmpty()) s"WHERE $where" else ""
  /**
   * returns a function mapping from Domains to CQL-Strings used in Where clauses
   */
  def getQueryMapping(store: S, storeTableNickName: Option[String]): DomainQuery => String = {
    (query) => {
      // first check that we have fixed all partition keys
      val r = getRowKeyQueryMapping(store, query, storeTableNickName).map { queryStringBegin =>
        // if this is the case the query can fix clustering keys and the last one may be a rangedomain
        val baseQuery = getClusterKeyQueryMapping(store, query, storeTableNickName) match {
          case None => queryStringBegin
          case Some(queryPart) => s"$queryStringBegin$AND_LITERAL$queryPart"
        }
        enforceLimit(baseQuery, query)
      }.getOrElse {
        // if there is not a partition and maybe a clustering column
        // we must make sure we have a single index for the col we select (only use one)
        enforceLimit(getValueKeyQueryMapping(store, query, storeTableNickName).getOrElse(""), query)
      }
      val result = s"""SELECT * FROM "${removeQuotes(store.ti.dbId)}"."${removeQuotes(store.ti.tableId)}" ${decideWhere(r)}"""
      logger.debug(s"Query String for Cassandra is $result")
      result
    }
  }

  /**
   * sets given limits at the provided query
   */
  private def enforceLimit(queryString: String, query: DomainQuery): String = {
    query.getQueryRange.map { range =>
      if(range.limit.isDefined) {
        val sbuf = new StringBuffer(queryString)
        val skip = range.skip.getOrElse(0L)
        sbuf.append(LIMIT_LITERAL).append(skip + range.limit.get).toString
      } else {
        queryString
      }
    }.getOrElse(queryString)
  }

  private def convertValue[T](value: T) = value match {
    case v: String => s"'$v' "
    case _ => {
        s"${value.toString} "
    }
  }

  // private def convertDomainToTargetDomain(domain: Domain[_]):

  private def convertSingleValueDomain(vdomain: SingleValueDomain[_]): String = {
    if(vdomain.isNull) {
      ""
    } else {
      s""" "${vdomain.column.columnName}"=${convertValue(vdomain.value)}"""
    }
  }

  private def convertRangeValueDomain(vdomain: RangeValueDomain[_]): String = {
    vdomain.lowerBound.map { bound =>
      val comp = bound.inclusive match {
        case true => ">="
        case false => ">"
      }
      s""" "${vdomain.column.columnName}" $comp ${convertValue(bound.value)}"""
    }.getOrElse("") + vdomain.upperBound.map { bound =>
      val and = vdomain.lowerBound.isDefined match {
        case true => AND_LITERAL
        case false => " "
      }
      val comp = bound.inclusive match {
        case true => "<="
        case false => "<"
      }
      s"""$and"${vdomain.column.columnName}" $comp ${convertValue(bound.value)}"""
    }.getOrElse("")
  }

  private def getRowKeyQueryMapping(store: S, query: DomainQuery,
      storeTableNickName: Option[String]): Option[String] = {
    val rowColumns = store.getRowKeyColumns
    val foundRowKeyDomains = rowColumns.flatMap(col => query.domains.filter { dom => dom match {
      case svd: SingleValueDomain[_] => svd.column.columnName == col.columnName &&
        compareCoordinatesWithNickname(svd.column.table, store, storeTableNickName)
      case _ => false
    }})
    if(foundRowKeyDomains.size == rowColumns.size) {
      Some(foundRowKeyDomains.map(svd => convertSingleValueDomain(svd.asInstanceOf[SingleValueDomain[_]])).mkString(AND_LITERAL))
    } else {
      None
    }
  }

  /**
   * efficiently compares TableIdentifiers, even under the presence of a storeNickName
   */
  @inline final private def compareCoordinatesWithNickname(tid: TableIdentifier, store: S, storeTableNickName: Option[String]): Boolean = {
    val lcoordinates = store.getScrayCoordinates
    val ltid = storeTableNickName.map(name => TableIdentifier(lcoordinates.dbSystem, lcoordinates.dbId, name)).getOrElse(lcoordinates)
    tid == ltid
  }
  
  /**
   * assuming that cols are in order of definition, which should be the case for Cassandra-stores
   * this recursion probably never overflows the stack as it is only on a few cols or domains
   */
  private def clusterColumnDomains(cols: Set[Column], store: S, query: DomainQuery, storeTableNickName: Option[String]): List[Domain[_]] = {
    if(cols == Nil) {
      Nil
    } else {
      // find relevant domain
      val domain = query.domains.find { dom =>
        dom.column.columnName == cols.head.columnName &&
        compareCoordinatesWithNickname(dom.column.table, store, storeTableNickName)
      }
      domain.collect {
        case svd: SingleValueDomain[_] => svd :: clusterColumnDomains(cols.tail, store, query, storeTableNickName)
        case rvd: RangeValueDomain[_] => rvd :: Nil
      }.getOrElse {
        Nil
      }
    }
  }

  private def getClusterKeyQueryMapping(store: S, query: DomainQuery, storeTableNickName: Option[String]): Option[String] = {
    val clusterCols = store.getClusteringKeyColumns
    val domains = clusterColumnDomains(clusterCols, store, query, storeTableNickName)
    // map the domains to CQL strings and AND this
    val cqlQuery = domains.collect {
      case svd: SingleValueDomain[_] => convertSingleValueDomain(svd)
      case rvd: RangeValueDomain[_] => convertRangeValueDomain(rvd)
    }.mkString(AND_LITERAL)
    cqlQuery.size match {
      case 0 => None
      case _ => Some(cqlQuery)
    }
  }

  private def getValueKeyQueryMapping(store: S, query: DomainQuery, storeTableNickName: Option[String]): Option[String] = {
    val valueCols = store.getValueColumns.map { valueCol => 
      Registry.getQuerySpaceColumn(query.getQueryspace, query.querySpaceVersion, valueCol)     
    }.filter(cd => cd.isDefined && cd.get.index.isDefined && cd.get.index.get.isAutoIndexed).
    partition(cd => cd.get.index.get.autoIndexConfiguration.isDefined)
    logger.trace(s"value Columns that are indexed: $valueCols")
    val resultQuery = if(valueCols._1.size > 0) {
      // if we have lucene entries, we use those as those are supposed to be more flexible
      logger.debug(s"Using Lucene index on ${valueCols._1}")
      val ti = valueCols._1.head.get.column.table
      val domains = query.getWhereAST.filter { dom =>
        valueCols._1.find{optcolDef => optcolDef.get.column.columnName == dom.column.columnName}.isDefined}
      DomainToJSONLuceneQueryMapper.getLuceneColumnsQueryMapping(query, domains, ti)
    } else {
      None
    }
    resultQuery.orElse {
      // if we a standard Cassandra index we use the first in the list of defined valuesColumns
      query.domains.find{dom => valueCols._2.find { valueColConf =>
        val valueCol = valueColConf.get.column
          dom.column.columnName == valueCol.columnName &&
          dom.isInstanceOf[SingleValueDomain[_]] &&
          compareCoordinatesWithNickname(dom.column.table, store, storeTableNickName)
      }.isDefined}.map(svd => convertSingleValueDomain(svd.asInstanceOf[SingleValueDomain[_]]))
    }
  }
}

object DomainToCQLQueryMapping {
  val EMPTY_LITERAL: String = ""
  val LIMIT_LITERAL: String = " LIMIT "
  val AND_LITERAL: String = " AND "
  val ORDER_LITERAL: String = " ORDER BY "
  val DESC_LITERAL: String = " DESC "
}

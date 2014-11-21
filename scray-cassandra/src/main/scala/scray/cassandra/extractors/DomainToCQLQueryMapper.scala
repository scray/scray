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
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.Domain
import scray.querying.description.Column
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.RangeValueDomain

/**
 * performs mapping of DomainQueries to valid Cassandra CQL queries,
 * containing all the possible predicates for a single table as defined
 * in the domains
 */
class DomainToCQLQueryMapper[S <: AbstractCQLCassandraStore[_, _]] {
  import DomainToCQLQueryMapper.AND_LITERAL
  
  /**
   * returns a function mapping from Domains to CQL-Strings used in Where clauses
   */
  def getQueryMapping(store: S, extractor: CassandraExtractor[S]): DomainQuery => String = {
    (query) => {
      // first check that we have fixed all partition keys
      getRowKeyQueryMapping(store, query, extractor).map { queryStringBegin =>
        // if this is the case the query can fix clustering keys and the last one may be a rangedomain 
        getClusterKeyQueryMapping(store, query, extractor) match {
          case None => queryStringBegin
          case Some(queryPart) => s"$queryStringBegin$AND_LITERAL$queryPart"
        }
      }.getOrElse {
        // if there is not a partition and maybe a clustering column 
        // we must make sure we have a single index for the col we select (only use one)
        getValueKeyQueryMapping(store, query, extractor).getOrElse("")
      }
    }
  }
  
  private def convertValue[T](value: T) = value match {
    case v: String => s"'$v' "
    case _ => s"$value " 
  }
  
  private def convertSingleValueDomain(vdomain: SingleValueDomain[_]): String = 
    s""" "${vdomain.column.columnName}"=${convertValue(vdomain.value)}"""
  
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
    }
  } 
    
  
  private def getRowKeyQueryMapping(store: S, query: DomainQuery, extractor: CassandraExtractor[S]): Option[String] = {
    val rowColumns = extractor.getRowKeyColumns
    val foundRowKeyDomains = rowColumns.flatMap(col => query.domains.filter { dom => dom match {
      case svd: SingleValueDomain[_] => svd.column.columnName == col.columnName && 
        svd.column.table.tableId == store.columnFamily.getName &&
        svd.column.table.dbId == store.columnFamily.session.getKeyspacename &&
        svd.column.table.dbSystem == extractor.getDBSystem
      case _ => false
    }})
    if(foundRowKeyDomains.size == rowColumns.size) {
      Some(foundRowKeyDomains.map(svd => convertSingleValueDomain(svd.asInstanceOf[SingleValueDomain[_]])).mkString(AND_LITERAL))
    } else {
      None
    }    
  }
  
  private def getClusterKeyQueryMapping(store: S, query: DomainQuery, extractor: CassandraExtractor[S]): Option[String] = {
    // assuming that cols are in order of definition, which should be the case for Cassandra-stores
    // this recursion probably never overflows the stack as it is only on a few cols or domains 
    def clusterColumnDomains(cols: List[Column]): List[Domain[_]] = {
      if(cols == Nil) { 
        Nil 
      } else {
        // find relevant domain
        val domain = query.domains.find { dom => 
          dom.column.columnName == cols.head.columnName && 
          dom.column.table.tableId == store.columnFamily.getName &&
          dom.column.table.dbId == store.columnFamily.session.getKeyspacename &&
          dom.column.table.dbSystem == extractor.getDBSystem
        }
        domain.collect { 
          case svd: SingleValueDomain[_] => svd :: clusterColumnDomains(cols.tail)
          case rvd: RangeValueDomain[_] => rvd :: Nil
        }.getOrElse {
          Nil
        }
      }
    }
    val clusterCols = extractor.getClusteringKeyColumns
    val domains = clusterColumnDomains(clusterCols)
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
  
  private def getValueKeyQueryMapping(store: S, query: DomainQuery, extractor: CassandraExtractor[S]): Option[String] = {
    val valueCols = extractor.getValueColumns.filter(valueCol => extractor.checkColumnCassandraAutoIndexed(store, valueCol))
    query.domains.find{dom => valueCols.find { valueCol => 
        dom.column.columnName == valueCol.columnName && 
        dom.isInstanceOf[SingleValueDomain[_]] &&
        dom.column.table.tableId == store.columnFamily.getName &&
        dom.column.table.dbId == store.columnFamily.session.getKeyspacename &&
        dom.column.table.dbSystem == extractor.getDBSystem
    }.isDefined}.map(svd => convertSingleValueDomain(svd.asInstanceOf[SingleValueDomain[_]]))
  }
}

object DomainToCQLQueryMapper {
  val AND_LITERAL: String = " AND "
}
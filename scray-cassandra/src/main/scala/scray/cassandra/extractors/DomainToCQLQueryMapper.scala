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

object DomainToCQLQueryMapper {

  private def convertValue[T](value: T) = value match {
    case v: String => s"'$v' "
    case _ => s"$value " 
  }
  
  private def convertSingleValueDomain(vdomain: SingleValueDomain[_]): String = 
    s""" "${vdomain.column.columnName}"=${convertValue(vdomain.value)}"""
  
  private def getRowKeyQueryMapping(store: AbstractCQLCassandraStore[_, _], query: DomainQuery, extractor: CassandraExtractor): Option[String] = {
    val rowColumns = extractor.getRowKeyColumns(store)
    val foundRowKeyDomains = rowColumns.flatMap(col => query.domains.filter { dom => dom match {
      case svd: SingleValueDomain[_] => svd.column.columnName == col && 
        svd.column.table.tableId == store.columnFamily.getName &&
        svd.column.table.dbId == store.columnFamily.session.getKeyspacename &&
        svd.column.table.dbSystem == extractor.getDBSystem
      case _ => false
    }})
    if(foundRowKeyDomains.size == rowColumns.size) {
      Some(foundRowKeyDomains.map(svd => convertSingleValueDomain(svd.asInstanceOf[SingleValueDomain[_]])).mkString(" AND "))
    } else {
      None
    }    
  }
  
  private def getClusterKeyQueryMapping(store: AbstractCQLCassandraStore[_, _], query: DomainQuery, extractor: CassandraExtractor): Option[String] = {
    // assume that cols are in order of definition, which should be the case for Cassandra-stores
    def clusterColumnDomains(cols: List[Column]): List[Domain[_]] = {
      if(cols == Nil) { 
        Nil 
      } else {
        // find relevant domain
        val domain = query.domains.find { dom => 
          dom.column.columnName == cols.head && 
          dom.column.table.tableId == store.columnFamily.getName &&
          dom.column.table.dbId == store.columnFamily.session.getKeyspacename &&
          dom.column.table.dbSystem == extractor.getDBSystem
        }
        domain.map { 
          case svd: SingleValueDomain[_] => svd :: clusterColumnDomains(cols.tail)
          case rvd: RangeValueDomain[_] => rvd :: Nil
        }.getOrElse {
          Nil
        }
      }
    }
    val clusterCols = extractor.getClusteringKeyColumns(store)
    val domains = clusterColumnDomains(clusterCols)
    // map the domains to CQL strings
    domains.collect {
      case svd: SingleValueDomain[_] => 
      case rvd: RangeValueDomain[_] => 
    }
    // AND this
  }
  
  
  def getQueryMapping(store: AbstractCQLCassandraStore[_, _], extractor: CassandraExtractor): DomainQuery => String = {
    (query) => {
      // first check that we have fixed all partition keys
      getRowKeyQueryMapping(store, query, extractor).map { queryStringBegin =>
        // if this is the case the query can fix clustering keys and the last one may be a rangedomain 
        ""
        
      }.getOrElse {
        // if there is not a partition and maybe a clustering column 
        // we must make sure we have a single index for the col we select (only use one)
        val valueCols = extractor.getValueColumns(store)
        valueCols.find { valueCol =>
          valueCol.columnName
        }.getOrElse {
          ""
        }
      }
    }
  }
}

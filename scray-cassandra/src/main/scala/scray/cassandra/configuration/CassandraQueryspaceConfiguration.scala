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
package scray.cassandra.configuration

import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreSession
import scray.querying.Query
import scray.querying.Registry
import scray.querying.planning.Planner
import scray.querying.description.ColumnConfiguration
import scray.querying.description.QueryspaceConfiguration
import scray.cassandra.extractors.CassandraExtractor
import scray.querying.description.Row
import scray.querying.description.Column
import scray.querying.source.indexing.IndexConfig
import scray.querying.description.TableConfiguration
import scray.querying.description.VersioningConfiguration
import scray.querying.description.internal.MaterializedView
import scray.querying.queries.DomainQuery
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.RangeValueDomain
import scala.annotation.tailrec
import scray.querying.source.Splitter

/**
 * configuration of a simple Cassandra-based query space.
 * If all tables used are based on Cassandra this class can be used out-of-the-box.
 * 
 * name is used to identify the new query space
 * tables is a list of Storehaus-Cassandra-stores you can use to query anything
 * indexes is a concrete list of things which are manually indexed
 * (auto-indexed columns are automatically detected for Cassandra)  
 */
class CassandraQueryspaceConfiguration(
    override val name: String,
    val tables: Set[(AbstractCQLCassandraStore[_, _], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _, _]]))],
    // mapping from indexed table and the indexed column to the table containing the index and the ref column
    indexes: Map[(AbstractCQLCassandraStore[_, _], String),
      (AbstractCQLCassandraStore[_, _], String, IndexConfig, Option[Function1[_,_]], Set[String])],
    splitters: Map[Column, Splitter[_]]
) extends QueryspaceConfiguration(name) {
  
  lazy val tableRowMapperMap: Map[AbstractCQLCassandraStore[_, _], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _, _]])] = tables.toMap
  
  override def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration] = {
    // TODO: maybe this is right for some cases, but we must implement all:
    //   - what happens if the manual index is not going to be used
    //   - what happens if the main query doesn't use the table with the clustering column 
    query.getOrdering.flatMap { ordering =>
      Registry.getQuerySpaceColumn(query.getQueryspace, query.querySpaceVersion, ordering.column).flatMap { colConfig =>
        colConfig.index.flatMap(index => if(index.isManuallyIndexed.isDefined && index.isSorted) {
          val itc = index.isManuallyIndexed.get.indexTableConfig()
          if(itc.queryableStore.isDefined || (itc.versioned.isDefined && itc.versioned.get.runtimeVersion().isDefined )) {
            Some(colConfig)
          } else {
            None
          }
        } else {
          None
        }).orElse {
          Registry.getQuerySpaceTable(query.getQueryspace, query.querySpaceVersion, ordering.column.table).flatMap { table =>
            if(table.clusteringKeyColumns.size > 0 && table.clusteringKeyColumns(0) == ordering.column ) {
              Some(colConfig)
            } else {
              None
            }
          }
        }
      }
    }
  }
  
  override def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration] = queryCanBeOrdered(query)
  
  override def getColumns(version: Int): List[ColumnConfiguration] = tables.toList.flatMap ( table => {
    // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
    val typeReducedTable = table._1.asInstanceOf[AbstractCQLCassandraStore[Any, Any]]
    val extractor = CassandraExtractor.getExtractor(typeReducedTable, table._2._2, table._2._3)
    val allColumns = extractor.getTableConfiguration(table._2._1).allColumns
    allColumns.map { col =>
      val index = extractor.createManualIndexConfiguration(col, name, version, typeReducedTable, indexes, tableRowMapperMap)
      extractor.getColumnConfiguration(typeReducedTable, col, this, index, splitters)
    }
  })
  
  override def getTables(version: Int): Set[TableConfiguration[_, _, _]] = tables.map ( table => {
    // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
    val typeReducedTable = table._1.asInstanceOf[AbstractCQLCassandraStore[Any, Any]]
    val extractor = CassandraExtractor.getExtractor(typeReducedTable, table._2._2, table._2._3)
    extractor.getTableConfiguration(table._2._1)
  })
  
  override def queryCanUseMaterializedView (query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)] = {
    @tailrec def calculateSpecificity(cols: List[Column], count: Int, checkOrdering: Option[Column] = None): (Boolean, Int) = {
      // returns true, specificity if view matches requested ordering
      // returns false, specificity if the view cannot be used to order according to ordering
      val single = query.getWhereAST.find { dom =>
         cols.size > 0 && dom.column == cols.head && dom.isInstanceOf[SingleValueDomain[_]]
      }.isDefined
      // last can be a RangeValueDomain (and should be in case of checkOrdering != None)
      if(!single) {
        val rangeResult: List[Option[Boolean]] = query.getWhereAST.collect { 
          case rvd: RangeValueDomain[_] if cols.size > 0 && rvd.column == cols.head => 
            checkOrdering match {
              case Some(orderedCol) => Some(orderedCol.columnName == cols.head) 
              case None => None
            }
        }
        if(rangeResult.size > 0) {
          rangeResult.head match {
            case Some(ord) => (ord, count + 1)
            case None => (false, count + 1)
          }
        } else {
          (false, count)          
        }
      } else {
        calculateSpecificity(cols.tail, count + 1, checkOrdering)
      }
    }
    def getClusteringMatches(): (Boolean, Int) = 
      calculateSpecificity(materializedView.viewTable.clusteringKeyColumns, 0, query.getOrdering.map(_.column))
    
    // check that all primarykeyColumns are satisfied with SingleValueDomains
    val partitionSatisfied = (materializedView.viewTable.primarykeyColumns.find ( col => query.getWhereAST.find { _ match {
      case svd: SingleValueDomain[_] => col != svd.column
      case _ => true
    }}.isDefined).isEmpty, materializedView.viewTable.primarykeyColumns.size)

    if(partitionSatisfied._1) {
      // the weight is calculated
      val clusteringMatches = getClusteringMatches
      Some(clusteringMatches._1, partitionSatisfied._2 + clusteringMatches._2)
    } else {
      None
    }
  }
  
  /**
   * reloads the query-space configuration
   * TODO: invent some mechanism to do the reload 
   */
  override def reInitialize(oldversion: Int): QueryspaceConfiguration = this 
}

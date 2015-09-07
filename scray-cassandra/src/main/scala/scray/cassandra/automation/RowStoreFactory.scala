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
package scray.cassandra.automation

import com.twitter.storehaus.cassandra.cql.{CQLCassandraConfiguration, CQLCassandraRowStore}
import com.websudos.phantom.CassandraPrimitive
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import scray.cassandra.util.CassandraUtils
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel}
import com.twitter.util.Duration
import scala.collection.JavaConverters._

/**
 * a factory for rows stores which reads table Metadata from Cassandra and builds CQLCassandraRowStores
 */
object RowStoreFactory {
  
  /**
   * create a row store object using metadata extraction of the datastax java driver
   */
  def getRowStore[K](cf: StoreColumnFamily,
          consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
          poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
          batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
          ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)(
                  implicit typeMap: Map[String, CassandraPrimitive[_]]): (Option[CQLCassandraRowStore[K]], List[(String, CassandraPrimitive[_])]) = {
    // retrieve table metadata
    val tableMeta = CassandraUtils.getTableMetadata(cf)
    val metaInfo = tableMeta.getColumns().asScala.map(colMeta => 
      (colMeta.getName(), typeMap.get(colMeta.getType().getName().toString()).get)).toList
    val keyColumnNames = tableMeta.getPartitionKey().asScala
    keyColumnNames.size match {
      case x: Int if x == 1 =>
        // create store
        val keySerializer = typeMap.get(keyColumnNames(0).getType().getName().toString()).get.asInstanceOf[CassandraPrimitive[K]]
        (Some(new CQLCassandraRowStore[K](cf, metaInfo, keyColumnNames(0).getName(), consistency, poolSize, batchType, ttl)(keySerializer)), metaInfo) 
      case _ => (None, metaInfo)
    }
  }
}

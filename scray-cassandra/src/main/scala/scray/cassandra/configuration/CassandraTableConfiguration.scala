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

import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import scray.cassandra.util.CassandraUtils
import scray.querying.description.Column
import scray.querying.description.Row
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import com.twitter.util.Try

object CassandraTableConfiguration {
  
  /**
   * property name of the property which holds the parallelization used to read and write the column family 
   */
  val PARALLELIZATION_TABLE_PROPERTY = "index_parallelization"
  
  /**
   * return an Option with the function to get the degree of parallelization used for this index table
   */
  def parallelizationFunction(): (QueryableStore[_, _]) => Option[Int] = (qstore) => {
    val cf = qstore.asInstanceOf[AbstractCQLCassandraStore[_, _]].columnFamily
    CassandraUtils.getTablePropertyFromCassandra(cf, PARALLELIZATION_TABLE_PROPERTY).flatMap(
        prop => Try(prop.toInt).toOption)
  }  
  
  /**
   * write the degree of parallelization used for this index table
   */
  def setParallelization(cf: StoreColumnFamily, parallelization: Int): Unit = 
      CassandraUtils.writeTablePropertyToCassandra(cf, PARALLELIZATION_TABLE_PROPERTY, parallelization.toString())
  
  /** 
   *  returns an ordering for time indexes
   */
  def timeIndexSingleColumnOrdering(col: Column): Option[(Row, Row) => Boolean] = Some {
    (row1, row2) => row1.getColumnValue[Long](col).map { row1val =>
      row2.getColumnValue[Long](col) match {
        case Some(row2val) => row1val < row2val
        case None => false 
      }
    }.getOrElse(true)
  }
}

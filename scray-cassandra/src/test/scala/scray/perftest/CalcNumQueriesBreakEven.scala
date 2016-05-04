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
package scray.perftest

import scopt.OptionParser
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.datastax.driver.core.querybuilder.Clause
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Select.Where
import com.datastax.driver.core.querybuilder.Select
import com.twitter.util.FuturePool
import scala.math.BigInt
import scala.util.Random

object CalcNumQueriesBreakEven {
  
  case class NumQueriesConfig(cassIP: String = "127.0.0.1", cassPort: Int = 9042, numberOfRows: Int = 0, keyspaceName: String = "SIL",
      columnFamilyName: String = "BISMTOlsWorkflowElement", keyColumnName: String = "key", partionerBits: Int = 64, numThreads: Int = 10)

  val parser = new OptionParser[NumQueriesConfig]("CalcNumQueriesBreakEven") {
    head("CalcNumQueriesBreakEven", "0.0.1-SNAPSHOT")
    opt[String]('i', "ip").action((x, c) => c.copy(cassIP = x)).text("IP of a single Cassandra node, default = 127.0.0.1")
    opt[Int]('p', "port").action((x, c) => c.copy(cassPort = x)).text("Native protocol port of Cassandra, default = 9042")
    opt[Int]('r', "num-rows").action((x, c) => c.copy(numberOfRows = x)).text("Number of rows to scan").required
    opt[String]('k', "keyspace").action((x, c) => c.copy(keyspaceName = x)).text("Name of the keyspace to query, default = SIL")
    opt[String]('c', "columnfamily").action((x, c) => c.copy(columnFamilyName = x)).
    	text("Name of the column Family to query, default = BISMTOlsWorkflowElement")
    opt[String]('y', "keycolumn").action((x, c) => c.copy(keyColumnName = x)).
    	text("Name of the key column to query, default = key")
    opt[Int]('b', "bits").action((x, c) => c.copy(partionerBits = x)).text("Number of bits of the partitioner hash function, default = 64")
    opt[Int]('t', "num-threads").action((x, c) => c.copy(numThreads = x)).text("Number of threads for concurrent execution of queries, default = 10")
    help("help").text("prints usage")
  }

  type QUERY = Clause => Select.Where
  
  def issueQueries(query: QUERY, number: Int, config: NumQueriesConfig, random: Random): Double = {
    // calculate limit
    val limit = config.numberOfRows / number
    val pool = FuturePool
    for(i <- 0 until config.numberOfRows) {
      // split 2^64 into "number" parts
      // project the limit to the number of the run using a number of size 2^bits - 1
      val token = BigInt(limit) * BigInt(i) * 
        (((BigInt(2) ^ BigInt(config.partionerBits)) - BigInt(1)) / BigInt(config.numberOfRows))
      query(QueryBuilder.gte(QueryBuilder.token(config.keyColumnName), token.toString)).limit(limit)
    }
    // TODO: fix this
    0.0d
  }
  
  def recursiveQuery(query: QUERY, number: Int, config: NumQueriesConfig, keyColumn: String, random: Random): List[Double] = {
    if(number > 0) {
      issueQueries(query, number, config, random) :: recursiveQuery(query, number / 2, config, keyColumn, random) 
    } else {
      Nil
    }
  }
  
  
  def main(args: Array[String]): Unit = {
    val rand = new Random
    parser.parse(args, NumQueriesConfig()).map{
      (config) => {
        // Initialize session
        val cluster = StoreCluster("Cluster", Set(StoreHost(config.cassIP + ":" + config.cassPort)))
        val session = StoreSession(config.keyspaceName, Some(cluster), None)
        val query: QUERY = QueryBuilder.select().all().from(config.columnFamilyName).where(_)
        // issue single query to pull all data into memory, if possible (to be able to compare)        
        val results = recursiveQuery(query, config.numberOfRows, config, config.keyColumnName, rand)
      } 
    }.getOrElse{
      println("Could not parse command line args!")
      println(parser.usage)
    }
  }
}

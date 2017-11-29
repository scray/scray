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

package scray.cassandra.sync

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import scala.collection.JavaConversions._
import scray.querying.sync.JobLockTable
import scray.querying.sync.DbSession
import scray.common.serialization.BatchID
import scray.querying.sync.JobInfo
import com.typesafe.scalalogging.LazyLogging
import scray.querying.sync.LockApi
import com.datastax.driver.core.{Cluster, ResultSet, Statement}
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder}
import scray.cassandra.util.CassandraUtils
import scray.common.serialization.BatchID
import scray.querying.sync.{DbSession, JobInfo, JobLockTable, LockApi}
import scray.querying.sync.conf.SyncConfiguration


class CassandraJobInfo(
    override val name: String,
    numberOfBatchSlots: Int = 3,
    numberOfOnlineSlots: Int = 2,
    dbSystem: String = "Cassandra",
    numberOfWorkersV: Option[Long] = None,
    lockTimeOut: Int = 500,
    syncConfV: SyncConfiguration = new SyncConfiguration) extends JobInfo[Statement, Insert, ResultSet](name, numberOfBatchSlots, numberOfOnlineSlots, numberOfWorkers = numberOfWorkersV, syncConf = syncConfV) with LazyLogging {

  import CassandraImplementation.genericCassandraColumnImplicit
  
  val statementGenerator = CassandraUtils

  def getLock(dbSession: DbSession[Statement, Insert, ResultSet, _]): LockApi[Statement, Insert, ResultSet] = {
     this.lock = this.lock.orElse {
      val table = JobLockTable("SILIDX", "JobSync")

      dbSession.execute(statementGenerator.createKeyspaceCreationStatement(table).get).
        recover {
          case e => { logger.error(s"Synctable is unable to create keyspace ${table.keySpace} Message: ${e.getMessage}"); throw e }
        }.flatMap { _ =>
          dbSession.execute(statementGenerator.createTableStatement(table).get)
        }

      // Register new job in lock table
      val insertQuery = QueryBuilder.insertInto(table.keySpace, table.tableName)
        .value(table.columns.jobname.name, this.name)
        .value(table.columns.locked.name, false)
        
      dbSession.execute(insertQuery)

      Some(new CassandraSyncTableLock(this, table, dbSession, lockTimeOut))
    }

    this.lock.get
  }

  def getLock(dbHostname: String): LockApi[Statement, Insert, ResultSet] = {
    this.getLock(new CassandraDbSession(Cluster.builder().addContactPoint(dbHostname).build().connect()))
  }
}

object CassandraJobInfo {
  def apply(name: String) = {
    new CassandraJobInfo(name)
  }
  
  def apply(name: String, batchID: BatchID, numberOfBatchVersions: Int, numberOfOnlineVersions: Int) = {
    new CassandraJobInfo(name, numberOfBatchVersions, numberOfOnlineVersions)
  }
}
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
package scray.jdbc.sync

import scala.collection.JavaConversions._
import scray.querying.sync.JobLockTable
import scray.querying.sync2.DbSession
import scray.common.serialization.BatchID
import scray.querying.sync.JobInfo
import scray.querying.sync.LockApi
import scray.querying.description.TableIdentifier
import java.sql.ResultSet
import com.typesafe.scalalogging.LazyLogging
import java.sql.PreparedStatement

/**
 * JDBC implementation of JobInfo
 */
class JDBCJobInfo(
    override val name: String,
    numberOfBatchSlots: Int = 3,
    numberOfOnlineSlots: Int = 2,
    dbSystem: String = "MariaDB",
    numberOfWorkersV: Option[Long] = None,
    lockTimeOut: Int = 500) extends JobInfo[PreparedStatement, PreparedStatement, ResultSet](name, numberOfBatchSlots, numberOfOnlineSlots, numberOfWorkers = numberOfWorkersV) with LazyLogging {

  // Not required for JDBC
  def getLock(dbSession: DbSession[PreparedStatement, PreparedStatement, ResultSet, _]): LockApi[PreparedStatement, PreparedStatement, ResultSet] = ???
  def getLock(dbHostname: String): LockApi[PreparedStatement, PreparedStatement, ResultSet] = ???
}

object JDBCJobInfo {
  def apply(name: String) = {
    new JDBCJobInfo(name)
  }
  
  def apply(name: String, numberOfBatchVersions: Int, numberOfOnlineVersions: Int) = {
    new JDBCJobInfo(name, numberOfBatchVersions, numberOfOnlineVersions)
  }
}
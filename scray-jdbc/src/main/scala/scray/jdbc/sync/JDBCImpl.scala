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

import java.sql.PreparedStatement
import java.sql.ResultSet

import scala.util.Try

import com.zaxxer.hikari.HikariConfig

import scray.jdbc.sync.tables.SyncTableComponent
import scray.querying.sync.DbSession
import scray.querying.sync.JobInfo
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.OnlineBatchSyncWithTableIdentifier
import scray.querying.sync.RowWithValue
import scray.querying.sync.StateMonitoringApi
import slick.lifted.CanBeQueryCondition._



class OnlineBatchSyncJDBC(dbSession: DbSession[PreparedStatement, PreparedStatement, ResultSet]) extends OnlineBatchSync[PreparedStatement, PreparedStatement, ResultSet] with OnlineBatchSyncWithTableIdentifier[PreparedStatement, PreparedStatement, ResultSet] with StateMonitoringApi[PreparedStatement, PreparedStatement, ResultSet] {

  def getBatchJobData[T <: RowWithValue, K](jobname: String, slot: Int, key: K, result: T): Option[scray.querying.sync.RowWithValue] = ???
  def getBatchJobData[T <: RowWithValue](jobInfo: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], result: T): Option[List[scray.querying.sync.RowWithValue]] = ???
  def getBatchJobData[T <: scray.querying.sync.RowWithValue](jobname: String, slot: Int, result: T): Option[List[scray.querying.sync.RowWithValue]] = ???

  def getOnlineJobData[T <: scray.querying.sync.RowWithValue](jobname: String, slot: Int, result: T): Option[List[scray.querying.sync.RowWithValue]] = ???
  def getOnlineStartTime(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): Option[Long] = ???
  def initJob[DataTableT <: scray.querying.sync.ArbitrarylyTypedRows](job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], dataTable: DataTableT): scala.util.Try[Unit] = ???
  def insertInBatchTable(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], data: scray.querying.sync.RowWithValue): scala.util.Try[Unit] = ???
  def setOnlineStartTime(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], time: Long): scala.util.Try[Unit] = ???
  def startInicialBatch(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], batchID: scray.common.serialization.BatchID): scala.util.Try[Unit] = ???
  def getLatestBatch(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement,java.sql.PreparedStatement,java.sql.ResultSet]): Option[Int] = ???
  def startNextBatchJob(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement,java.sql.PreparedStatement,java.sql.ResultSet]): scala.util.Try[Unit] = ???
  // Members declared in scray.querying.sync.OnlineBatchSyncWithTableIdentifier
  def completeBatchJob(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): scala.util.Try[Unit] = ???
  def completeOnlineJob(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): scala.util.Try[Unit] = ???
  def getBatchVersion(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def getOnlineVersion(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def getQueryableTableIdentifiers: List[(String, scray.querying.description.TableIdentifier, Int)] = ???
  def getTableIdentifierOfRunningJob(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def initJob[DataTableT <: scray.querying.sync.ArbitrarylyTypedRows](job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): scala.util.Try[Unit] = ???
  def startNextBatchJob(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], dataTable: scray.querying.description.TableIdentifier): scala.util.Try[Unit] = ???
  def startNextOnlineJob(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet]): scala.util.Try[Unit] = ???

  // Members declared in scray.querying.sync.StateMonitoringApi
  def getBatchJobState(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], slot: Int): Option[scray.querying.sync.State.State] = ???
  def getOnlineJobState(job: scray.querying.sync.JobInfo[java.sql.PreparedStatement, java.sql.PreparedStatement, java.sql.ResultSet], slot: Int): Option[scray.querying.sync.State.State] = ???

}

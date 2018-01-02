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

import scray.jdbc.sync.tables.SyncTableComponent
import scray.querying.sync.AbstractRow
import scray.querying.sync.StateMonitoringApi
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.RowWithValue
import scray.querying.sync.JobInfo
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.DbSession
import scray.querying.sync.OnlineBatchSyncWithTableIdentifier
import scray.querying.sync.State
import java.sql.PreparedStatement
import java.sql.ResultSet
import scala.util.Try
import slick.jdbc.JdbcProfile
import scala.util.Failure
import scala.util.Success
import scray.querying.description.TableIdentifier
import scray.querying.sync.types.BatchMetadata
import com.typesafe.scalalogging.LazyLogging
import java.sql.SQLSyntaxErrorException

class OnlineBatchSyncJDBC(dbSession: JDBCDbSession) extends OnlineBatchSync[PreparedStatement, PreparedStatement, ResultSet] with OnlineBatchSyncWithTableIdentifier[PreparedStatement, PreparedStatement, ResultSet] with StateMonitoringApi[PreparedStatement, PreparedStatement, ResultSet] with LazyLogging {

  val comp = new SyncTableComponent(dbSession.getConnectionInformations.get)
  import comp.SyncTableT

  def getLatestQueryableBatchData(jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = {

    val completedJob = dbSession.execute(comp.getLatestCompletedJobStatement(jobInfo, false))

    if (completedJob.size > 0) {
      val latestCompletedJobs = completedJob.head
      Some(TableIdentifier(latestCompletedJobs.dbSystem, latestCompletedJobs.dbId, latestCompletedJobs.tableID))
    } else {
      None
    }
  }

  def getQueryableBatchData(jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): List[scray.querying.description.TableIdentifier] = {
    List.empty
  }

  def initJobIfNotExists[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {
    Try({
      (
        dbSession.execute(comp.tableExists),
        dbSession.execute(comp.jobWasRegistered(job)))
    }).map { case (tableExists, jobWasRegistered) => tableExists & jobWasRegistered }
    match {
      case Success(jobExists) => {  
        if(!jobExists) {
          this.initJob(job)
        } else {
          Try()
        }
      }
      case Failure(e) => e match {
        case e: SQLSyntaxErrorException => {
          if(e.getMessage.contains("doesn't exist")) {
            this.initJob(job)  
          } else {
            Failure(e)
          }
        }
        case e: Throwable => Failure(e)
      }
    }
  }

  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {

    // Create sync tables 
    val createdTable = Try(comp.create.statements.map { statement => dbSession.execute(statement) })

    // Register job
    val registedJobs = createdTable.flatMap { _ =>
      Try(comp.registerJobStatement(job).map(statement => dbSession.execute(statement)))
    }

    registedJobs match {
      case Success(_) => Try()
      case Failure(ex) => {
        logger.warn(s"Error while initialising job ${job}. Exception: ${ex.getMessage}\n ${ex.printStackTrace()}")
        Failure(ex)
      }
    }
  }

  def getLatestBatchMetadata(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[BatchMetadata] = {
    val completedJob = dbSession.execute(comp.getLatestCompletedJobStatement(job, false))

    if (completedJob.size < 1) {
      None
    } else {
      Some(completedJob.head).map { newestBatchRow => BatchMetadata(newestBatchRow.batchStartTime, newestBatchRow.batchEndTime) }
    }
  }

  def startNextBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {
    Try(dbSession.execute(comp.getLatestCompletedJobStatement(job, false)))
      .map { completedJob =>

        // If it is the first job start with slot 0
        if (completedJob.size < 1) {
          Try(dbSession.execute(comp.startJobStatement(job, 0, false)))
        } else {
          Try(dbSession.execute(comp.startJobStatement(job, (completedJob.head.slot + 1) % job.numberOfBatchSlots, false)))
        }
      }
  }

  def getRunningBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Option[Int]] = {
    Try(dbSession.execute(comp.getRunningJobStatement(job, false)))
      .map { runningJobs =>
        {
          if (runningJobs.size > 0) {
            Some(runningJobs.head.slot)
          } else {
            None
          }
        }
      }
  }

  // Members declared in OnlineBatchSyncWithTableIdentifier
  def completeBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {
    this.completeBatchJob(job, System.currentTimeMillis())
  }

  def completeBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], batchEndTime: Long = System.currentTimeMillis()): Try[Unit] = {
    this.getRunningBatchJob(job).map { slot =>
      slot.map { slot => dbSession.execute(comp.completeJobStatement(job, slot, false, batchEndTime)) }
    }
  }
  override def insertInBatchTable(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], data: RowWithValue): Try[Unit] = ???
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = ???
  def getOnlineStartTime(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[Long] = ???
  def initJob[DataTableT <: AbstractRow](job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], dataTable: DataTableT): Try[Unit] = ???
  def getBatchJobData[T <: RowWithValue, K](jobname: String, slot: Int, key: K, result: T): Option[RowWithValue] = ???
  def getBatchJobData[T <: RowWithValue](jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet], result: T): Option[List[RowWithValue]] = ???
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = ???
  def setOnlineStartTime(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], time: Long): Try[Unit] = ???
  def startInicialBatch(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], batchID: scray.common.serialization.BatchID): Try[Unit] = ???
  def getLatestBatch(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[Int] = ???
  def completeOnlineJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = ???
  def getBatchVersion(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def getOnlineVersion(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def getQueryableTableIdentifiers: List[(String, scray.querying.description.TableIdentifier, Int)] = ???
  def getTableIdentifierOfRunningJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def startNextBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], dataTable: scray.querying.description.TableIdentifier): Try[Unit] = ???
  def startNextOnlineJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = ???

  // Members declared in StateMonitoringApi
  def getBatchJobState(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], slot: Int): Option[State.State] = ???
  def getOnlineJobState(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], slot: Int): Option[State.State] = ???

}

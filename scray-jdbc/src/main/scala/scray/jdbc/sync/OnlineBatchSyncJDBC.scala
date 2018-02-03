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
import scray.jdbc.sync.tables.ScrayStreamingStartTimesIO
import scray.jdbc.sync.tables.ScrayStreamingStartTimesDb
import scray.jdbc.sync.tables.ScrayStreamingStartTimes
import java.util.LinkedList
import scala.collection.JavaConverters._

class OnlineBatchSyncJDBC[StartPointT](dbSession: JDBCDbSession) extends OnlineBatchSync[PreparedStatement, PreparedStatement, ResultSet] with OnlineBatchSyncWithTableIdentifier[PreparedStatement, PreparedStatement, ResultSet] with StateMonitoringApi[PreparedStatement, PreparedStatement, ResultSet] with LazyLogging {

  val syncTable = new SyncTableComponent(dbSession.getConnectionInformations.get)
  val streamingStartTimeTable = new ScrayStreamingStartTimesIO(dbSession.getConnectionInformations.get)
  import syncTable.SyncTableT

  def getLatestQueryableBatchData(jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = {
    dbSession.execute(syncTable.getLatestCompletedJobStatement(jobInfo, false))
      .map(completedJob => {
        if (completedJob.size > 0) {
          val latestCompletedJobs = completedJob.head
          Some(TableIdentifier(latestCompletedJobs.dbSystem, latestCompletedJobs.dbId, latestCompletedJobs.tableID))
        } else {
          None
        }
      }) match {
        case Success(ti) => ti
        case Failure(e)  => logger.error(s"Unable to get latest queryable batch data. ${e}"); None
      }
  }

  def getQueryableBatchData(jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): List[scray.querying.description.TableIdentifier] = {
    List.empty
  }

  def initJobIfNotExists[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {

    dbSession.execute(syncTable.tableExists).map(tableExists =>
      {
        if (tableExists) {
          dbSession.execute(syncTable.jobWasRegistered(job)) match {
            case Failure(e) => e match {
              case e: SQLSyntaxErrorException => {
                if (e.getMessage.contains("doesn't exist")) {
                  this.initJob(job)
                } else {
                  Failure(e)
                }
              }
              case e: Throwable => Failure(e)
            }
          }
        } else {
          this.initJob(job)
        }
      }) match {
      case Failure(e) => e match {
        case e: SQLSyntaxErrorException => {
          if (e.getMessage.contains("doesn't exist")) {
            println("affe 1")
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
    val createdTable = Try(syncTable.create.statements.map { statement => dbSession.execute(statement) })
    dbSession.execute(streamingStartTimeTable.create)

    // Register job
    val registedJobs = createdTable.flatMap { _ =>
      Try(syncTable.registerJobStatement(job).map(statement => dbSession.execute(statement)))
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
    dbSession.execute(syncTable.getLatestCompletedJobStatement(job, false))
      .map(completedJob =>
        if (completedJob.size < 1) {
          None
        } else {
          Some(completedJob.head).map { newestBatchRow => BatchMetadata(newestBatchRow.batchStartTime, newestBatchRow.batchEndTime) }
        }) match {
        case Success(metadata) => metadata
        case Failure(ex) => {
          logger.warn(s"Error while reading meatadata from db. Exception: ${ex.getMessage}\n ${ex.printStackTrace()}")
          None
        }
      }
  }

  def startNextBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {
    dbSession.execute(syncTable.getLatestCompletedJobStatement(job, false))
      .map { completedJob =>

        // If it is the first job start with slot 0
        if (completedJob.size < 1) {
          Try(dbSession.execute(syncTable.startJobStatement(job, 0, false)))
        } else {
          Try(dbSession.execute(syncTable.startJobStatement(job, (completedJob.head.slot + 1) % job.numberOfBatchSlots, false)))
        }
      }
  }

  def startNextOnlineJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {
    dbSession.execute(syncTable.getLatestCompletedJobStatement(job, true))
      .map { completedJobs =>

        // If it is the first job start with slot 0
        if (completedJobs.size < 1) {
          Try(dbSession.execute(syncTable.startJobStatement(job, 0, true)))
        } else {
          val newSlot = (completedJobs.head.slot + 1) % job.numberOfOnlineSlots
          // Clean up slot
          dbSession.execute(streamingStartTimeTable.truncateSlot(job, newSlot))
          Try(dbSession.execute(syncTable.startJobStatement(job, newSlot, true)))
        }
      }
  }

  def getRunningBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Option[Int]] = {
    dbSession.execute(syncTable.getRunningJobStatement(job, false))
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

  def getRunningOnlineJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Option[Int]] = {
    dbSession.execute(syncTable.getRunningJobStatement(job, true))
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
      slot.map { slot => dbSession.execute(syncTable.completeJobStatement(job, slot, false, batchEndTime)) }
    }
  }

  def completeOnlineJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Try[Unit] = {
    this.getRunningOnlineJob(job).map { slot =>
      slot.map { slot => dbSession.execute(syncTable.completeJobStatement(job, slot, true, System.currentTimeMillis())) }
    }
  }

  def setOnlineStartPoint(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], time: Long, startPoint: Long): Try[Unit] = {
    dbSession.execute(syncTable.getRunningJobStatement(job, true))
      .map { runningOnlineJobs =>
        if (runningOnlineJobs.size == 0) {
          logger.error(s"No running online job exists")
        }

        if (runningOnlineJobs.size == 1) {
          dbSession.execute(streamingStartTimeTable.setStartTime(job, runningOnlineJobs.head.slot, time, startPoint.toString()))
        }
      }
  }

  def setOnlineStartPoint(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], time: Long, startPoint: String): Try[Unit] = {
    dbSession.execute(syncTable.getRunningJobStatement(job, true))
      .map { runningOnlineJobs =>
        if (runningOnlineJobs.size == 0) {
          logger.error(s"No running online job exists")
        }

        if (runningOnlineJobs.size == 1) {
          dbSession.execute(streamingStartTimeTable.setStartTime(job, runningOnlineJobs.head.slot, time, startPoint))
        }
      }
  }

  def getOnlineStartPointAsInt(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], slot: Int): java.util.List[ScrayStreamingStartTimes[Int]] = {
    ScrayStreamingStartTimes[Long]("", 1, 1L, 1L)
    dbSession.execute(streamingStartTimeTable.getSartTimes(job, slot))
      .map { startPoint =>

        startPoint.foldLeft(List[ScrayStreamingStartTimes[Int]]())(
          (acc, startPoint) => {
            ScrayStreamingStartTimes[Int](startPoint.jobname, startPoint.slot, startPoint.timestamp, startPoint.startPoint.toInt) :: acc
          })
      } match {
        case Success(startPoints) => startPoints.asJava
        case Failure(e)           => new LinkedList()
      }
  }

  def getOnlineStartPointAsString(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], slot: Int): java.util.List[ScrayStreamingStartTimes[String]] = {
    dbSession.execute(streamingStartTimeTable.getSartTimes(job, slot))
      .map { startPoint =>

        startPoint.foldLeft(List[ScrayStreamingStartTimes[String]]())(
          (acc, startPoint) => {
            ScrayStreamingStartTimes[String](startPoint.jobname, startPoint.slot, startPoint.timestamp, startPoint.startPoint) :: acc
          })
      } match {
        case Success(startPoints) => startPoints.asJava
        case Failure(e)           => new LinkedList()
      }
  }

  override def insertInBatchTable(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], data: RowWithValue): Try[Unit] = ???
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = ???
  def getOnlineStartTime(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[Long] = ???
  def initJob[DataTableT <: AbstractRow](job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], dataTable: DataTableT): Try[Unit] = ???
  def getBatchJobData[T <: RowWithValue, K](jobname: String, slot: Int, key: K, result: T): Option[RowWithValue] = ???
  def getBatchJobData[T <: RowWithValue](jobInfo: JobInfo[PreparedStatement, PreparedStatement, ResultSet], result: T): Option[List[RowWithValue]] = ???
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = ???
  def startInicialBatch(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], batchID: scray.common.serialization.BatchID): Try[Unit] = ???
  def getLatestBatch(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[Int] = ???
  def getBatchVersion(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def getOnlineVersion(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def getQueryableTableIdentifiers: List[(String, scray.querying.description.TableIdentifier, Int)] = ???
  def getTableIdentifierOfRunningJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet]): Option[scray.querying.description.TableIdentifier] = ???
  def startNextBatchJob(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], dataTable: scray.querying.description.TableIdentifier): Try[Unit] = ???

  // Members declared in StateMonitoringApi
  def getBatchJobState(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], slot: Int): Option[State.State] = ???
  def getOnlineJobState(job: JobInfo[PreparedStatement, PreparedStatement, ResultSet], slot: Int): Option[State.State] = ???

}

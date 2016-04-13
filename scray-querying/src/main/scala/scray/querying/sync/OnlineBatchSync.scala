package scray.querying.sync

import java.util.{ Iterator => JIterator }
import scray.querying.sync.types.State.State
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import scray.querying.sync.types._
import scray.querying.sync.types._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scray.querying.description.TableIdentifier
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.common.serialization.BatchID


abstract class OnlineBatchSync extends LazyLogging {

  /**
   * Generate and register tables for a new job.
   */
  def initJob[T <: ArbitrarylyTypedRows](job: JobInfo, dataTable: T): Try[Unit]
  
  def startNextBatchJob(job: JobInfo): Try[Unit]
  def startNextOnlineJob(job: JobInfo): Try[Unit]
  
  def completeBatchJob(job: JobInfo): Try[Unit]
  def completeOnlineJob(job: JobInfo): Try[Unit]
  
  def resetBatchJob(job: JobInfo): Try[Unit]
  def resetOnlineJob(job: JobInfo): Try[Unit]
  
  def getRunningBatchJobSlot(job: JobInfo): Option[Int]
  def getRunningOnlineJobSlot(job: JobInfo): Option[Int]
  
  def insertInBatchTable(jobName: JobInfo, slot: Int, data: RowWithValue): Try[Unit]
  def insertInOnlineTable(jobName: JobInfo, slot: Int, data: RowWithValue): Try[Unit]
  
  def getOnlineJobState(job: JobInfo, slot: Int): Option[State]
  def getBatchJobState(job: JobInfo, slot: Int): Option[State]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  
  def getNewestOnlineSlot(job: JobInfo): Option[Int]
  def getNewestBatchSlot(job: JobInfo): Option[Int]  
  
  def getLatestBatch(job: JobInfo): Option[Int] 
}


class JobInfo(
  val name: String,
  val batchID: BatchID,
  val numberOfBatchSlots: Int = 3,
  val numberOfOnlineSlots: Int = 2
  ) extends Serializable {}

object JobInfo {
 def apply(name: String, batchID: BatchID) = {
    new JobInfo(name, batchID)
  }
  def apply(name: String, batchID: BatchID, numberOfBatchVersions: Int, numberOfOnlineVersions: Int) = {
    new JobInfo(name, batchID, numberOfBatchVersions, numberOfOnlineVersions)
  }
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)
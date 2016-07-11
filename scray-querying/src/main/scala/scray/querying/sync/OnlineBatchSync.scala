package scray.querying.sync

import java.util.{ Iterator => JIterator }
import scray.querying.sync.State.State
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
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scray.querying.description.TableIdentifier
import com.typesafe.scalalogging.slf4j.LazyLogging


abstract class OnlineBatchSync extends LazyLogging with Serializable {

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
  
  def getRunningBatchJobVersion(job: JobInfo): Option[Int]
  def getRunningOnlineJobVersion(job: JobInfo): Option[Int]
  
  def insertInBatchTable(jobName: JobInfo, nr: Int, data: RowWithValue): Try[Unit]
  def insertInOnlineTable(jobName: JobInfo, nr: Int, data: RowWithValue): Try[Unit]
  
  def getOnlineJobState(job: JobInfo, version: Int): Option[State]
  def getBatchJobState(job: JobInfo, version: Int): Option[State]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, nr: Int, result: T): Option[List[RowWithValue]]
  def getBatchJobData[T <: RowWithValue](jobname: String, nr: Int, result: T): Option[List[RowWithValue]]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
}


class JobInfo(
  val name: String,
  val numberOfBatcheVersions: Int = 3,
  val numberOfOnlineVersions: Int = 2
  ) extends Serializable {}

object JobInfo {
 def apply(name: String) = {
    new JobInfo(name)
  }
  def apply(name: String, numberOfBatcheVersions: Int, numberOfOnlineVersions: Int) = {
    new JobInfo(name, numberOfBatcheVersions, numberOfOnlineVersions)
  }
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)
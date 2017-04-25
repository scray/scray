package scray.cassandra.sync

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

import org.slf4j.LoggerFactory

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.typesafe.scalalogging.slf4j.Logger

import scray.cassandra.sync.CassandraImplementation.genericCassandraColumnImplicit
import scray.cassandra.util.CassandraUtils
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.querying.sync.DBColumnImplementation
import scray.querying.sync.DbSession
import scray.querying.sync.JobInfo
import scray.querying.sync.Table
import shapeless.syntax.singleton._

/**
 * Find a consensus about the start time of a job.
 * Start time is the time of the first consumed element. This can be an attribute of the element.
 * It is assumed that all elements, consumed by one instance are ordered in an increasing order. (The first element is the oldest element)
 * Also other ordinal attributes can be use.
 */
class StartTimeDetector(job: JobInfo[Statement, Insert, ResultSet],
                        dbSession: DbSession[Statement, Insert, ResultSet]) extends LazyLogging {

  val startConsensusTable = new Table("silidx", "startconsensus", new StartConsensusRow)
  var valueAlreadySet = false

  def this(job: JobInfo[Statement, Insert, ResultSet], dbHostname: String) {
    this(job, new CassandraDbSession(Cluster.builder().addContactPoint(dbHostname).build().connect()))
  }
  
  /**
   * Create keyspaces and tables if needed.
   */
  def init = {
    logger.debug("Init StartTimeDetector")
    CassandraUtils.createKeyspaceCreationStatement(startConsensusTable).map { statement => dbSession.execute(statement) }
    CassandraUtils.createTableStatement(startConsensusTable).map { statement => dbSession.execute(statement) }
  }

  def allNodesVoted: Option[Long] = {
    val votes = QueryBuilder.select.all().from(startConsensusTable.keySpace, startConsensusTable.tableName).where(
      QueryBuilder.eq(startConsensusTable.columns.jobname.name, job.name))

    // Check if all worker voted
    val allWorkerVoted = dbSession.execute(votes)
      .map { _.all() }.recover {
        case e => { logger.error(s"DB error while fetching all element of the vote for ${job.name}. Error: ${e.getMessage}"); throw e }
      }.toOption.map { rows =>
        if (rows.size() > 0) {
          logger.debug(rows.size() + " note(s) sent start time")
          ((rows.get(0).getInt(startConsensusTable.columns.numberOfWorkers.name) <= rows.size()), rows)
        } else {
          logger.debug("No node sent start time")
          (false, rows)
        }
      }.map { voteResult =>
        if (voteResult._1) {
          logger.debug("All nodes sent first element time")
          Some(getMinTime(voteResult._2))
        } else {
          logger.debug("Not all workes pulished the first element time. Try it again later")
          None
        }
      }

    return allWorkerVoted.flatten
  }

  def publishLocalStartTime(time: Long): Try[Boolean] = {
    val statement = job.numberOfWorkers.map { numWorkers =>
      QueryBuilder.insertInto(startConsensusTable.keySpace, startConsensusTable.tableName)
        .value(startConsensusTable.columns.jobname.name, job.name)
        .value(startConsensusTable.columns.id.name, java.util.UUID.randomUUID.toString)
        .value(startConsensusTable.columns.time.name, time)
        .value(startConsensusTable.columns.numberOfWorkers.name, numWorkers)
    }

    statement match {
      case Some(statement) => Try(dbSession.insert(statement).isSuccess)
      case None            => logger.warn("No numberOfWorkers configured! No time update"); throw new RuntimeException("No numberOfWorkers configured! No time update")
    }
  }
  
  def publishLocalStartTimeOnlyOnce(time: Long) = {
    if(valueAlreadySet == false) {
      val wasSuccesfull = publishLocalStartTime(time)
    }
  }
  
  /**
   * Reset all sent start times for the given job.
   */
  def resetSentStartTimes: Try[Boolean] = {
    val statement =  QueryBuilder.delete.from(startConsensusTable.keySpace, startConsensusTable.tableName)
    .where(
        QueryBuilder.eq(startConsensusTable.columns.jobname.name, job.name)
     )
     
    Try(dbSession.execute(statement).isSuccess)
  } 

  /**
   * Get the time of the first element.
   */
  def getMinTime(times: java.util.List[Row]): Long = {

    times.asScala.foldLeft(Long.MaxValue)((min, row) => {
      if (row.getLong(startConsensusTable.columns.time.name) < min) {
        row.getLong(startConsensusTable.columns.time.name)
      } else {
        min
      }
    })
  }

  /**
   * Poll the database until all nodes sent their first element date.
   * 
   * @param timeout timeout value in seconds to wait for first element time
   */
  def waitForFirstElementTime(timeout: Int): Option[Long] = {

    val pollingTask: Callable[Long]  = new Callable[Long] {
      
      val logger = Logger(LoggerFactory.getLogger(this.getClass))
      val sleepTimeBetweenPolling = 5000 // ms

      def poll = {
        allNodesVoted match {
          case Some(time) => time
          case None       => 0
        }
      }
      
      def call(): Long =  {
        // poll db
        println(poll)
        while (poll < 1) {
          logger.debug(s"No first element time found. Poll again in ${sleepTimeBetweenPolling}ms")
          Thread.sleep(sleepTimeBetweenPolling)
        }
      
        val time = poll
        logger.info(s"Found first element time ${time}")
        time
      }
    }
    
    // Wait for result
    val firstElementTime = try {
      val thread = Executors.newSingleThreadExecutor;   
      Some(thread.submit(pollingTask).get(timeout, TimeUnit.SECONDS))
    } catch {
      case e: TimeoutException => { logger.error(s"Timeout while waiting for time of first element"); None }
      case e: Throwable => {logger.error(s"Error while waiting for start time. ${e.getMessage} ${e.printStackTrace}"); None}
    }

     return firstElementTime
    }
    
    
  
  def waitForFirstElementTime: Option[Long] = {
    waitForFirstElementTime(480)
  }

  class StartConsensusRow(implicit colString: DBColumnImplementation[String],
                          colInt: DBColumnImplementation[Int]) extends ArbitrarylyTypedRows {

    val jobname = new Column[String]("jobname")
    val id = new Column[String]("id")
    val time = new Column[Long]("time")
    val numberOfWorkers = new Column[Int]("numWorkers")

    override val columns = jobname :: id :: time :: numberOfWorkers :: Nil
    override val primaryKey = s"(${jobname.name}, ${id.name})"
    override val indexes: Option[List[String]] = None
  }

}




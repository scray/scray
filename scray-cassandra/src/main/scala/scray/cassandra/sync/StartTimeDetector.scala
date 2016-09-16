package scray.cassandra.sync

import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.querying.sync.DBColumnImplementation
import scray.querying.sync.JobInfo
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import scray.cassandra.util.CassandraUtils
import scray.querying.sync.DbSession
import com.datastax.driver.core.querybuilder.QueryBuilder
import scray.querying.sync.State
import scray.cassandra.sync.CassandraImplementation.genericCassandraColumnImplicit
import scray.querying.sync.Table
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.datastax.driver.core.Row
import scala.collection.JavaConverters._

/**
 * Find a consensus about the start time of a job.
 * Start time is the time of the first consumed element. This can be an attribute of the element.
 * It is assumed that all elements, consumed by one instance are ordered in an increasing order. (The first element is the oldest element)
 * Also other ordinal attributes can be use.
 */
object StartTimeDetector extends LazyLogging {

  val startConsensusTable = new Table("silidx", "startconsensus", new StartConsensusRow)

  def allNodesVoted(job: JobInfo[Statement, Insert, ResultSet], dbSession: DbSession[Statement, Insert, ResultSet]): Option[Long] = {
    val votes = QueryBuilder.select.all().from(startConsensusTable.keySpace, startConsensusTable.tableName).where(
      QueryBuilder.eq(startConsensusTable.columns.jobname.name, job.name))

    // Check if all worker voted
    val allWorkerVoted = dbSession.execute(votes)
      .map { _.all() }.recover {
        case e => { logger.error(s"DB error while fetching all element of the vote for ${job.name}. Error: ${e.getMessage}"); throw e }
      }.toOption.map { rows =>
        if (rows.size() > 0) {
          ((rows.get(0).getLong(startConsensusTable.columns.numberOfWorkers.name) <= rows.size()), rows)
        } else {
          (false, rows)
        }
      }.map { voteResult =>
        if (voteResult._1) {
          logger.debug("All nodes sended first element time")
          Some(getMinTime(voteResult._2))
        } else {
          logger.debug("Not all workes pulished the first element time. Try it again later")
          None
        }
      }
      
    return allWorkerVoted.flatten
  }

  /**
   * Get the time of the first element.
   */
  def getMinTime(times: java.util.List[Row]): Long = {

    times.asScala.foldLeft(Long.MaxValue)((min, row) => {
      if (row.getLong(startConsensusTable.columns.numberOfWorkers.name) < min) {
        row.getLong(startConsensusTable.columns.numberOfWorkers.name)
      } else {
        min
      }
    })
  }


  class StartConsensusRow(implicit colString: DBColumnImplementation[String],
                          colLong: DBColumnImplementation[Long],
                          colInt: DBColumnImplementation[Int]) extends ArbitrarylyTypedRows {

    val jobname = new Column[String]("jobname")
    val time = new Column[Long]("time")
    val numberOfWorkers = new Column[Int]("numWorkers")

    override val columns = jobname :: time :: numberOfWorkers :: Nil
    override val primaryKey = s"(${jobname.name}, ${time.name})"
    override val indexes: Option[List[String]] = None
  }

}




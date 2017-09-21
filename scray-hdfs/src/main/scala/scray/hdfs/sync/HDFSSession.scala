package scray.hdfs.sync

import scray.querying.sync.DbSession
import scray.querying.sync.StatementExecutionError
import java.sql.PreparedStatement
import scala.util.Failure
import scala.util.Success
import java.sql.ResultSet
import scala.util.Try
import com.typesafe.scalalogging.LazyLogging

/**
 * Session object used only for compatibility
 */
class HDFSSession(val directory: String) extends DbSession[Nothing,Nothing,Nothing](directory) with LazyLogging{
  // ??? is by intention, as these methods have no meaning in the context of plain HDFS  
  def execute(statement: Nothing): Try[Nothing] = Try[Nothing] { ??? }
  def execute(statement: String): Try[Nothing] = Try[Nothing] { ??? }
  def insert(statement: Nothing): Try[Nothing] = Try[Nothing] { ??? }
}
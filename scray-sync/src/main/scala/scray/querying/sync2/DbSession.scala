package scray.querying.sync2

import scala.util.Try

trait DbSession[Statement, InsertIn, Result, ConnectionInformations] {
  def execute(statement: Statement): Try[Result]
  def execute(statement: String): Try[_]
  def insert(statement: InsertIn): Try[Result]
  def getConnectionInformations: Option[ConnectionInformations] = None
}
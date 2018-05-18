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

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import scray.jdbc.extractors.MariaDBDialect
import scray.jdbc.extractors.ScrayH2Dialect
import scray.jdbc.extractors.ScraySQLDialect
import scray.jdbc.extractors.ScraySQLDialectFactory
import slick.dbio.DBIOAction
import slick.jdbc.JdbcProfile
import slick.sql.FixedSqlAction
import scray.querying.sync.StatementExecutionError
import scray.querying.sync.DbSession
import scray.jdbc.extractors.ScrayHiveDialect
import java.sql.DriverManager

/**
 * Session implementation for JDBC datasources using a Hikari connection pool
 */
trait JDBCDbSession extends DbSession[PreparedStatement, PreparedStatement, ResultSet, JdbcProfile] {

  override def getConnectionInformations: Option[JdbcProfile]

  def executeQuery(statement: String): Try[ResultSet]

  def execute[A, B <: slick.dbio.NoStream, C <: Nothing](statement: FixedSqlAction[A, B, C]): Try[A]

  def execute[A, S <: slick.dbio.NoStream, E <: slick.dbio.Effect](statement: DBIOAction[A, S, E])

  override def insert(statement: PreparedStatement): Try[ResultSet]

  def printStatement(s: Statement): String
}

object JDBCDbSession {
  def getNewJDBCDbSession(ds: HikariDataSource, sqlDialiect: ScraySQLDialect) = {
    Try(new JDBCDbSessionImpl(ds, sqlDialiect))
  }

  def getNewJDBCDbSession(jdbcURL: String, username: String, password: String) = {
    Try(new JDBCDbSessionImpl(jdbcURL, username, password))
  }
}
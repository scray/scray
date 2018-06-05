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

package scray.cassandra.sync

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.BatchStatement
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scray.querying.sync2.DbSession
import com.typesafe.scalalogging.LazyLogging
import scray.querying.sync.StatementExecutionError
import com.datastax.driver.core.Cluster
import scray.querying.sync.StatementExecutionError

import scala.util.{Failure, Success, Try}

class CassandraDbSession(val cassandraSession: Session) extends DbSession[Statement, Insert, ResultSet, String]() with LazyLogging{
  
  def this(host: String) = {
    this(Cluster.builder().addContactPoint(host).build().connect())
  }
  
  override def execute(statement: String): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: Statement): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def insert(statement: Insert): Try[ResultSet] = {
      try {
        logger.debug("Insert " + statement)
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: SimpleStatement): Try[ResultSet] = {
       try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }
    
    def printStatement(s: Statement): String = {
      s match {
         case bStatement: BatchStatement => "It is currently not possible to execute : " + bStatement.getStatements
         case _                          => "It is currently not possible to execute : " + s
      }
    }
}

//// See the LICENCE.txt file distributed with this work for additional
//// information regarding copyright ownership.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package scray.jdbc.sync.helpers
//
//
//import scala.util.{Failure, Success, Try}
//import java.sql.ResultSet
//import scray.querying.sync.ArbitrarylyTypedRows
//import com.twitter.finagle.builder.Cluster
//import shapeless.ops.zipper.Insert
//import scray.querying.sync.StatementExecutionError
//import scray.querying.sync.DbSession
//import scray.querying.sync.Column
//import java.sql.Statement
//import java.sql.PreparedStatement
//import scray.jdbc.sync.JDBCImplementation._
//import com.typesafe.scalalogging.LazyLogging
//
//
//  /**
//   * Test columns
//   */
//  class SumTestColumns() extends ArbitrarylyTypedRows {
//    val sum = new Column[Long]("sum")
//
//    override val columns = sum :: Nil
//    override val primaryKey = s"(${sum.name})"
//    override val indexes: Option[List[String]] = None
//  }
//
//  class TestDbSession extends DbSession[Statement, PreparedStatement, ResultSet]("127.0.0.1") with LazyLogging {
//    
//    
//    
//    override def execute(statement: String): Try[ResultSet] = {
////      val result = cassandraSession.execute(statement)
////      if (result.wasApplied()) {
////        Success(result)
////      } else {
//        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}."))
////      }
//    }
//
//    def execute(statement: Statement): Try[ResultSet] = {
//      logger.debug("Execute: " +  statement)
////      val result = cassandraSession.execute(statement)
////      if (result.wasApplied()) {
////        Success(result)
////      } else {
//        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}."))
////      }
//    }
//
//    def insert(statement: PreparedStatement): Try[ResultSet] = {
//      logger.debug("Execute: " +  statement)
////      val result = cassandraSession.execute(statement)
////      if (result.wasApplied()) {
////        Success(result)
////      } else {
//        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}."))
////      }
//    }
//    
//    def cleanDb = ???
//  }
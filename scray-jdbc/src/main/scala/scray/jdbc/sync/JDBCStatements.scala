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

import scray.querying.sync.AbstractRow
import scray.querying.sync.Table
import java.sql.PreparedStatement

/**
 * A collection of queries which are used to synchronize batch and online jobs
 */
object JDBCStatements {
  
   def createTableStatement[T <: AbstractRow](table: Table[T]): Option[String] = {
     val sql = s"CREATE TABLE ${table.tableName} (" +
     s"${table.columns.foldLeft("")((acc, next) => { acc + next.name + " " + next.getDBType + ", " })} " +
     s")"
     
     Some(sql)
   }
}
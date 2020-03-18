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
package scray.querying.queries

import java.util.UUID
import scray.querying.description.{Clause, Columns, ColumnGrouping, ColumnOrdering, QueryRange, TableIdentifier}
import scray.querying.Query

case class SimpleQuery(
    space: String,
    table: TableIdentifier,
    id: UUID = UUID.randomUUID,
    columns: Columns = Columns(Left(true)), // simple default meaning all columns
    where: Option[Clause] = None, // None means no conditions specified, return all results
    grouping: Option[ColumnGrouping] = None, // None means no grouping
    ordering: Option[ColumnOrdering[_]] = None, // None means ordering undefined
    range: Option[QueryRange] = None // None means no range specified
    ) extends Query {

  def getQueryID: UUID = id
  
  override def getQueryspace: String = space
  
  override def getResultSetColumns: Columns = columns
  
  override def getTableIdentifier: TableIdentifier = table
  
  override def getWhereAST: Option[Clause] = where
  
  override def getGrouping: Option[ColumnGrouping] = grouping
  
  override def getOrdering: Option[ColumnOrdering[_]] = ordering
  
  override def getQueryRange: Option[QueryRange] = range
  
  override def transformedAstCopy(ast: Option[Clause]): SimpleQuery = SimpleQuery(space, table, id, columns, ast, grouping, ordering, range)
}

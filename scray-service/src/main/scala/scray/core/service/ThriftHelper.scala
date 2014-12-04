
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
package scray.core.service

import scray.service.base.thrifscala._
import scray.service.qmodel.thrifscala._
import java.nio.ByteBuffer
import java.util.UUID

object ThriftHelper {

  /**
   * Creates minimal thrift query meta data
   * No column info and no values given, these need to be part of the query expression
   */
  def createSQuery(queryExpr : String, querySpace : String, dbSystem : String, dbId : String, tableId : String, queryId : UUID = UUID.randomUUID()) : ScrayTQuery =
    ScrayTQuery(
      queryInfo = ScrayTQueryInfo(
        queryId = Some(ScrayUUID(queryId.getLeastSignificantBits(), queryId.getMostSignificantBits())),
        querySpace = querySpace,
        tableInfo = ScrayTTableInfo(
          dbSystem = dbSystem,
          dbId = dbId,
          tableId = tableId,
          keyT = ScrayTTypeInfo(
            ScrayTType.Any,
            None /* Key Class */ )),
        columns = Set()),
      values = Map(),
      queryExpression = queryExpr)

}
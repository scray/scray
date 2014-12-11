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

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import org.parboiled2.ParseError
import com.twitter.util.Future
import scray.core.service.parser.TQueryParser
import scray.core.service.spools.SpoolRack
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.service.qservice.thrifscala.ScrayTResultFrame
import scray.core.service.spools.SpoolPager
import scray.service.qservice.thrifscala.ScrayTService
import scray.core.service.spools.ServiceSpool
import com.twitter.concurrent.Spool
import scray.querying.description.Row

object ScrayTServiceImpl extends ScrayTServiceImpl(SpoolRack)

class ScrayTServiceImpl(val rack : SpoolRack) extends ScrayTService.FutureIface {

  def query(tQuery : ScrayTQuery) : Future[ScrayUUID] = {
    val parser = new TQueryParser(tQuery)
    val parsed = parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        sys.error(parser.formatError(e, showTraces = true)); Failure(e)
      case Failure(e) => throw e
    }
    parsed.flatMap(_.createQuery) match {
      case Success(query) => Future.value(rack.createSpool(query, tQuery.queryInfo).queryId.get)
      case Failure(ex) => Future.exception(ex)
    }
  }

  def getResults(queryId : ScrayUUID) : Future[ScrayTResultFrame] = {
    rack.getSpool(queryId) match {
      case Some(spool) => {
        // create a future page
        new SpoolPager(spool) page () map { pair =>
          ScrayTResultFrame(
            // update spool and use its updated query info (new ttl)
            rack.updateSpool(queryId, ServiceSpool(pair._2, spool.tQueryInfo)).tQueryInfo,
            pair._1)
        }
      }
      case None => Future.exception(new ScrayServiceException(
        ExceptionIDs.SPOOLING_ERROR, Some(queryId), s"No results for query ${ScrayUUID2UUID(queryId)}.", None))
    }
  }

}

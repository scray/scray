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
import scray.service.qservice.thrifscala.ScrayStatefulTService
import scray.core.service.spools.ServiceSpool
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import org.slf4j.LoggerFactory
import scray.core.service.spools.TSpoolRack
import com.typesafe.scalalogging.LazyLogging

object ScrayStatefulTServiceImpl {
  def apply() = new ScrayStatefulTServiceImpl(TSpoolRack)
}

class ScrayStatefulTServiceImpl(val rack : SpoolRack) extends ScrayStatefulTService.FutureIface with LazyLogging {

  def query(tQuery : ScrayTQuery) : Future[ScrayUUID] = {
    logger.info(s"Query: ${tQuery._3}")
    logger.trace(s"New 'query' request: ${tQuery}")

    val parser = new TQueryParser(tQuery)
    val parsed = parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        logger.error(parser.formatError(e, showTraces = true)); Failure(e)
      case Failure(e) => throw e
    }
    parsed.flatMap(_.createQuery) match {
      case Success(query) => try { Future.value(rack.createSpool(query, tQuery.queryInfo).queryId.get) } catch { case e: Exception => e.printStackTrace(); throw e}
      case Failure(ex) => Future.exception(ex)
    }
  }

  def getResults(queryId : ScrayUUID) : Future[ScrayTResultFrame] = {

    logger.trace(s"New 'getResults' request: ${queryId}")

    rack.getSpool(queryId) match {
      case Some(spool) => {
        val snap = System.currentTimeMillis()
        // create a future page (and remaining spool)
        val pair = new SpoolPager(spool) page ()
        logger.trace(s"Spooling for $queryId took ${System.currentTimeMillis() - snap}")
        // create result frame with updated query info 
        pair.map(pair => ScrayTResultFrame(
          rack.updateSpool(queryId, ServiceSpool(pair._2, spool.tQueryInfo)).tQueryInfo, pair._1))
      }
      case None => Future.exception(new ScrayServiceException(
        ExceptionIDs.SPOOLING_ERROR, queryId, s"No results for query ${ScrayUUID2UUID(queryId)}."))
    }
  }

}

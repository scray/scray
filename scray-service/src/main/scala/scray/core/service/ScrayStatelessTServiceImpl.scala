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
import scray.service.qservice.thrifscala.ScrayStatelessTService
import scray.core.service.spools.ServiceSpool
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import org.slf4j.LoggerFactory
import scray.core.service.spools.{ PageRack, PageKey, PageValue }
import scray.common.serialization.KryoPoolSerialization
import scray.querying.description.EmptyRow
import scray.service.qmodel.thrifscala.ScrayTColumn
import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import scray.core.service.spools.RowConverter
import scray.core.service.spools.memcached.MemcachedPageRack
import scray.core.service.spools.MPageRack

object ScrayStatelessTServiceImpl {
  def apply() = new ScrayStatelessTServiceImpl(MPageRack)
}

class ScrayStatelessTServiceImpl(val rack : PageRack) extends ScrayStatelessTService.FutureIface {

  private val logger = LoggerFactory.getLogger(classOf[ScrayStatelessTServiceImpl])

  def query(tQuery : ScrayTQuery) : Future[ScrayUUID] = {

    logger.info(s"New 'query' request: ${tQuery}")

    val parser = new TQueryParser(tQuery)
    val parsed = parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        logger.error(parser.formatError(e, showTraces = true)); Failure(e)
      case Failure(e) => throw e
    }
    parsed.flatMap(_.createQuery) match {
      case Success(query) => Future.value(rack.createPages(query, tQuery.queryInfo).queryId.get)
      case Failure(ex) => Future.exception(ex)
    }
  }

  def getResults(queryId : ScrayUUID, pageIndex : Int) : Future[ScrayTResultFrame] = {

    logger.info(s"New 'getResults' request: ${queryId}")

    val snap = System.currentTimeMillis()
    val frame = rack.getPage(PageKey(queryId, pageIndex)).flatMap {
      _ match {
        case Some(pval) => Future.value(ScrayTResultFrame(pval.tQueryInfo, pval.page.map(RowConverter.convertRow(_))))
        case None => Future.exception(new ScrayServiceException(
          ExceptionIDs.CACHING_ERROR, Some(queryId), s"No results for query ${ScrayUUID2UUID(queryId)} on page ${pageIndex}.", None))
      }
    }
    logger.debug(s"Page retrieval for $queryId page $pageIndex took ${System.currentTimeMillis() - snap}")
    return frame
  }

}

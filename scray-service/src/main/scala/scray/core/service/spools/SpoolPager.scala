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

package scray.core.service.spools

import java.nio.ByteBuffer
import scala.annotation.tailrec
import com.twitter.concurrent.Spool
import com.twitter.util.Future
import scray.common.serialization.KryoPoolSerialization
import scray.core.service._
import scray.querying.description.Column
import scray.querying.description.EmptyRow
import scray.querying.description.Row
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.service.qmodel.thrifscala.ScrayTColumn
import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import scray.querying.Registry
import java.util.UUID
import com.typesafe.scalalogging.LazyLogging
import com.twitter.util.Time

/**
 * Slice a result spool into pages.
 * Use page() to get one page of results together with a spool holding the rest of the results.
 * Use pageAll() to get a lazy spool of all pages.
 */
class SpoolPager(sspool: ServiceSpool) extends LazyLogging {

  val pagesize: Int = sspool.tQueryInfo.pagesize.getOrElse(DEFAULT_PAGESIZE)

  // fetches a page worth of of converted rows and returns it paired with the remaining spool
  def page(): Future[(Seq[ScrayTRow], Spool[Row])] = if (!sspool.spool.isEmpty) {
    part(Seq(sspool.spool.head), sspool.spool.tail, pagesize - 1).map { pair => (pair._1 map { RowConverter.convertRow(_) }, pair._2) }
  } else {
    Future.value((Seq(ScrayTRow(None, None)), sspool.spool))
  }

  def pageAll(): Future[Spool[Seq[Row]]] = pageAll(sspool.spool)

  // lazily fetch spool rows chunked as pages
  private def pageAll(spool: Spool[Row]): Future[Spool[Seq[Row]]] = if (!spool.isEmpty) {
    part(Seq(spool.head), spool.tail, pagesize - 1) flatMap { pair => pageAll(pair._2) map (pair._1 **:: _) }
  } else {
    logFin(System.currentTimeMillis())
    Future.value(Seq[Row](new SucceedingRow()) **:: Spool.empty)
  }

  // recursive function parting a given spool into an eager head (Page part) and a lazy tail (Spool part)
  private def part(dest: Seq[Row], src: Future[Spool[Row]], pos: Int): Future[(Seq[Row], Spool[Row])] =
    if (pos <= 0) {
      logFin(System.currentTimeMillis())
      src.map { (dest, _) }
    } else {
      src.flatMap { spool =>
        if (!spool.isEmpty) {
          part(dest :+ spool.head, spool.tail, pos - 1)
        } else {
          logFin(System.currentTimeMillis())
          Future.value((dest :+ new SucceedingRow(), Spool.empty))
        }
      }
    }

  def logFin(end: Long) = {
    sspool.tQueryInfo.queryId.map(sid => new UUID(sid.mostSigBits, sid.leastSigBits)).map { uuid =>
      logger.debug(s"Finished query ${uuid} at ${end}.")
      Registry.getQueryInformation(uuid).map(_.finished.set(end))
    }
  }
}

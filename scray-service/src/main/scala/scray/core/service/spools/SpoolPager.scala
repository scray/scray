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

/**
 * Marker row demarcating the end of the result set (within a page)
 */
class SucceedingSpoolRow extends EmptyRow

class SpoolPager(sspool : ServiceSpool) {

  final val DEFAULT_PAGESIZE : Int = 50
  val pagesize : Int = sspool.tQueryInfo.pagesize.getOrElse(DEFAULT_PAGESIZE)

  // fetches a page worth of of converted rows and returns it paired with the remaining spool
  def page() : Future[(Seq[ScrayTRow], Spool[Row])] = if (!sspool.spool.isEmpty) {
    part(Seq(sspool.spool.head), sspool.spool.tail, pagesize - 1).map { pair => (pair._1 map { convertRow(_) }, pair._2) }
  } else {
    Future.value((Seq(ScrayTRow(None, None)), sspool.spool))
  }

  // recursive function parting a given spool into an eager head (Seq part) and a lazy tail (Spool part)
  private def part(dest : Seq[Row], src : Future[Spool[Row]], pos : Int) : Future[(Seq[Row], Spool[Row])] =
    if (pos <= 0) {
      src.map { (dest, _) }
    } else {
      src.flatMap { spool =>
        if (!spool.isEmpty) {
          part(dest :+ spool.head, spool.tail, pos - 1)
        } else {
          Future.value((dest :+ new SucceedingSpoolRow(), Spool.empty))
        }
      }
    }

  private def convertRow(sRow : Row) : ScrayTRow = sRow match {
    case sRow : SucceedingSpoolRow => ScrayTRow(None, None)
    case _ => ScrayTRow(None, Some(sRow.getColumns.map { col =>
      ScrayTColumn(ScrayTColumnInfo(col.columnName, None, None), encode(sRow.getColumnValue(col).get))
    }))
  }

  private def encode[V](value : V) : ByteBuffer =
    ByteBuffer.wrap(KryoPoolSerialization.chill.toBytesWithClass(value))

}

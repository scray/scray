package scray.core.service.util

import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import java.nio.ByteBuffer
import scray.service.qmodel.thrifscala.ScrayTTableInfo
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import java.util.UUID
import scray.common.serialization.KryoPoolSerialization
import scala.util.Random
import scray.core.service.UUID2ScrayUUID
import scala.collection.breakOut

trait TQuerySamples {
  val PAGESIZE = 10

  val tcols = 1.until(10).map(i => ScrayTColumnInfo(s"col$i"))

  val tbli = ScrayTTableInfo("myDbSystem", "myDbId", "myTableId", None)

  def qryi(ps : Int = PAGESIZE) = ScrayTQueryInfo(Some(UUID.randomUUID()), "myQuerySpace", tbli, tcols, Some(ps), None)

  def vals(buff : () => ByteBuffer) : Map[String, ByteBuffer] = 1.until(10).map(i => (s"val$i" -> buff()))(breakOut)
  val nullbuff = () => ByteBuffer.allocate(10)
  val kryoStrbuff = () => ByteBuffer.wrap(KryoPoolSerialization.chill.toBytesWithClass(Random.nextString(10)))
  val kryoLongbuff = () => ByteBuffer.wrap(KryoPoolSerialization.chill.toBytesWithClass(Random.nextLong()))

  def createTQuery(expr : String, pagesize : Int = PAGESIZE, buff : () => ByteBuffer = nullbuff) =
    ScrayTQuery(qryi(pagesize), vals(buff), expr)
}

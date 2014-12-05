package scray.core.service

import java.util.UUID
import com.twitter.util.Future
import scray.service.base.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.service.qservice.thrifscala.ScrayTResultFrame
import scray.service.qservice.thrifscala.ScrayTService
import java.nio.ByteBuffer

object ScrayTServiceDummyImpl extends ScrayTService.FutureIface with ScrayTQueryObj {

  val queryObj : ScrayTQuery = getTQueryObj("SELECT @nothing FROM @nowhere")
  val id = UUID.randomUUID()

  def query(query : ScrayTQuery) : Future[ScrayUUID] = Future.value(id)

  def getResults(queryId : ScrayUUID, offset : Int) : Future[ScrayTResultFrame] = {
    val queryInfo : ScrayTQueryInfo = queryObj.queryInfo
    val offset : Int = 1
    val rows : Seq[ScrayTRow] = Seq(ScrayTRow(ByteBuffer.allocate(10), Map("foo" -> ByteBuffer.allocate(10))))
    val frame = ScrayTResultFrame(queryInfo, offset, rows)
    Future.value(frame)
  }
}

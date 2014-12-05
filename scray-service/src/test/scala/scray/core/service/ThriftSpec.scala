package scray.core.service

import java.nio.ByteBuffer
import java.util.UUID

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.Thrift
import com.twitter.util._

import scray.service.base.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.service.qservice.thrifscala.ScrayTResultFrame
import scray.service.qservice.thrifscala.ScrayTService

@RunWith(classOf[JUnitRunner])
class ThriftSpec extends FlatSpec with Matchers with BeforeAndAfter with ScrayTQueryObj {

  val ENDPOINT = "localhost:8080"

  val server = Thrift.serveIface(ENDPOINT, ServiceImpl)

  before {
    println("Thrift server bound to: " + server.boundAddress)
  }

  after {
    server.close()
  }

  "scray service query method" should "return a query id" in {
    val queryObj : ScrayTQuery = getTQueryObj("SELECT @nothing FROM @nowhere")

    // prepare client
    val client = Thrift.newIface[ScrayTService.FutureIface](ENDPOINT)

    //call service
    val res = client.query(queryObj) onFailure { e => throw e } onSuccess { r => println(s"Received '$r'.") }

    Await.result(res) shouldBe a[ScrayUUID]
  }

}

object ServiceImpl extends ScrayTService.FutureIface with ScrayTQueryObj {

  val queryObj : ScrayTQuery = getTQueryObj("SELECT @nothing FROM @nowhere")
  val id = UUID.randomUUID()

  def query(query : ScrayTQuery) : Future[ScrayUUID] =
    Future.value(ScrayUUID(id.getLeastSignificantBits(), id.getMostSignificantBits()))

  def getResults(queryId : ScrayUUID, offset : Int) : Future[ScrayTResultFrame] = {
    val queryInfo : ScrayTQueryInfo = queryObj.queryInfo
    val offset : Int = 1
    val rows : Seq[ScrayTRow] = Seq(ScrayTRow(ByteBuffer.allocate(10), Map("foo" -> ByteBuffer.allocate(10))))
    val frame = ScrayTResultFrame(queryInfo, offset, rows)
    Future.value(frame)
  }
}
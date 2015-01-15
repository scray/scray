package scray.core.service

import com.twitter.finagle.Thrift
import com.twitter.util.Future
import scray.service.qmodel.thrifscala.ScrayTQuery
import java.nio.ByteBuffer
import scray.service.qservice.thrifscala.ScrayStatefulTService
import com.twitter.util.Await
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTTableInfo
import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import java.util.UUID
import scala.collection.breakOut
import scray.common.serialization.KryoPoolSerialization
import scala.util.Random
import scray.core.service.util.TQuerySamples

object ScrayTServiceTestClient extends TQuerySamples {
  def main(args : Array[String]) {

    val queryObj : ScrayTQuery = createTQuery(expr = "SELECT nothing FROM @myTableId")

    // prepare client
    val client = Thrift.newIface[ScrayStatefulTService.FutureIface](ENDPOINT)

    //call service
    val res = client.query(queryObj) onFailure { e => throw e } onSuccess { r => println(s"Received '$r'.") }

    Await.result(res)
    println(res.get)
  }
}

package scray.core.service

import com.twitter.finagle.Thrift
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qservice.thrifscala.ScrayStatefulTService
import com.twitter.util.Await
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

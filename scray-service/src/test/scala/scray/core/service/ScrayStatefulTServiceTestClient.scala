package scray.core.service

import com.twitter.finagle.Thrift
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qservice.thrifscala.ScrayStatefulTService
import com.twitter.util.Await
import scray.core.service.util.TQuerySamples

import scray.core.service.properties.ScrayServicePropertiesRegistration

object ScrayTServiceTestClient extends TQuerySamples {
  def main(args : Array[String]) {

    ScrayServicePropertiesRegistration.configure()

    val queryObj : ScrayTQuery = createTQuery(expr = "SELECT nothing FROM @myTableId")

    // prepare client
    val client = Thrift.newIface[ScrayStatefulTService.FutureIface]("10.11.22.37:18181")

    //call service
    val res = client.query(queryObj) onFailure { e => throw e } onSuccess { r => println(s"Received '$r'.") }

    Await.result(res)
    println(res.get)
  }
}

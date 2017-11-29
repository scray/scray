package scray.core.service

import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.twitter.concurrent.Spool
import com.twitter.finagle.Thrift
import com.twitter.util.Await
import scray.querying.description.Row
import scray.querying.planning.Planner
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qservice.thrifscala.ScrayStatefulTService
import scray.core.service.util.{ SpoolSamples, TQuerySamples }
import scray.querying.Query
import scray.core.service.spools.TimedSpoolRack
import scray.common.properties.ScrayProperties
import scray.common.properties.ScrayProperties.Phase
import scray.core.service.properties.ScrayServicePropertiesRegistration

@RunWith(classOf[JUnitRunner])
class ThriftSpec
  extends FlatSpec
  with Matchers
  with BeforeAndAfter
  with TQuerySamples
  with MockitoSugar
  with KryoPoolRegistration
  with SpoolSamples {

  ScrayServicePropertiesRegistration.configure()

  // prepare back end (query engine) mock
  val mockplanner = mock[(Query) => Spool[Row]]
  object MockedSpoolRack extends TimedSpoolRack(planAndExecute = mockplanner)
  when(mockplanner(anyObject())).thenReturn(Await.result(spool1))

  // prepare finagle
  object TestService extends ScrayStatefulTServiceImpl(MockedSpoolRack)
  val server = Thrift.server.serveIface(inetAddr2EndpointString(SCRAY_QUERY_LISTENING_ENDPOINT), TestService)
  val client = Thrift.client.newIface[ScrayStatefulTService.FutureIface](inetAddr2EndpointString(SCRAY_QUERY_HOST_ENDPOINT))

  before {
    // register kryo serializers
    registerSerializers
    println("Thrift server bound to: " + server.boundAddress)
  }

  after {
    server.close()
  }

  "scray service query method" should "return a query id" in {
    val queryObj: ScrayTQuery = createTQuery("SELECT @col1 FROM @myTableId")

    //call service
    val res = client.query(queryObj) onFailure { e => throw e } onSuccess { r => println(s"Received '$r'.") }

    Await.result(res) shouldBe a[ScrayUUID]
  }

}

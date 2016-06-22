package scray.core.service

import com.twitter.finagle.Thrift
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qservice.thrifscala.ScrayStatefulTService
import com.twitter.util.Await
import scray.core.service.util.TQuerySamples

import scray.core.service.properties.ScrayServicePropertiesRegistration
import scray.common.serialization.KryoPoolSerialization
import java.util.UUID
import scala.util.Random
import java.nio.ByteBuffer
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import scray.service.qmodel.thrifscala.ScrayTTableInfo
import scala.collection.breakOut

object ScrayTServiceTestClient extends TQuerySamples {

  object TQuerySamples2 {
      val PAGESIZE = 10
    
      val tcols = 1.until(10).map(i => ScrayTColumnInfo(s"col$i"))
    
      val tbli = ScrayTTableInfo("cassandra", "SIL", "BISMTOlsWf", None)
    
      def qryi(ps : Int = PAGESIZE) = ScrayTQueryInfo(Some(UUID.randomUUID()), "SIL", tbli, tcols, Some(ps), None)
    
      def vals(buff : () => ByteBuffer) : Map[String, ByteBuffer] = 1.until(10).map(i => (s"val$i" -> buff()))(breakOut)
      val nullbuff = () => ByteBuffer.allocate(10)
      val kryoStrbuff = () => ByteBuffer.wrap(KryoPoolSerialization.chill.toBytesWithClass(Random.nextString(10)))
      val kryoLongbuff = () => ByteBuffer.wrap(KryoPoolSerialization.chill.toBytesWithClass(Random.nextLong()))
    
      def createTQuery(expr : String, pagesize : Int = PAGESIZE, buff : () => ByteBuffer = nullbuff) =
        ScrayTQuery(qryi(pagesize), vals(buff), expr)
}

  
  def main(args : Array[String]) {

    ScrayServicePropertiesRegistration.configure()

    val queryObj : ScrayTQuery = TQuerySamples2.createTQuery(expr = "SELECT * FROM @BISMTOlsWf LIMIT 1 TIMEOUT 12s")

    // prepare client
    val client = Thrift.newIface[ScrayStatefulTService.FutureIface](
        s"10.11.22.38:18181")

    //call service
    val res = client.query(queryObj) onFailure { e => throw e } onSuccess { r => println(s"Received '$r'.") }

    Await.result(res)
    println(res.get)
  }
}


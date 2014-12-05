package scray.core.service

import com.twitter.finagle.Thrift
import com.twitter.util.Future
import scray.service.qmodel.thrifscala.ScrayTQuery
import java.nio.ByteBuffer
import scray.service.qservice.thrifscala.ScrayTService
import com.twitter.util.Await

object ScrayTServiceTestClient extends ScrayTQueryObj {
  def main(args : Array[String]) {

    val queryObj : ScrayTQuery = getTQueryObj("SELECT @nothing FROM @nowhere")

    // prepare client
    val client = Thrift.newIface[ScrayTService.FutureIface](ENDPOINT)

    //call service
    val res = client.query(queryObj) onFailure { e => throw e } onSuccess { r => println(s"Received '$r'.") }

    Await.result(res)    
    println(res.get)
  }
}

trait ScrayTQueryObj {
  def getTQueryObj(expr : String) = ScrayTQuery(
    queryInfo = scray.service.qmodel.thrifscala.ScrayTQueryInfo(
      queryId = Some(scray.service.base.thrifscala.ScrayUUID(1, 2)),
      querySpace = "myQuerySpace",
      tableInfo = scray.service.qmodel.thrifscala.ScrayTTableInfo(
        dbSystem = "myDbSystem",
        dbId = "myDbId",
        tableId = "myTableId",
        keyT = scray.service.base.thrifscala.ScrayTTypeInfo(
          scray.service.base.thrifscala.ScrayTType(1),
          Some("scray.classname"))),
      columns = Set(
        scray.service.qmodel.thrifscala.ScrayTColumnInfo(
          name = "col1",
          None /* STypeInfo */ ,
          None /* STableInfo */ ),
        scray.service.qmodel.thrifscala.ScrayTColumnInfo(
          name = "col2",
          None /* STypeInfo */ ,
          None /* STableInfo */ ))),
    values = Map(
      "val1" -> scray.service.base.thrifscala.ScrayTValue(
        scray.service.base.thrifscala.ScrayTTypeInfo(
          scray.service.base.thrifscala.ScrayTType(2),
          Some("scray.classname")),
        ByteBuffer.allocate(10)),
      "val2" -> scray.service.base.thrifscala.ScrayTValue(
        scray.service.base.thrifscala.ScrayTTypeInfo(
          scray.service.base.thrifscala.ScrayTType(2),
          Some("scray.classname")),
        ByteBuffer.allocate(10)),
      "val3" -> scray.service.base.thrifscala.ScrayTValue(
        scray.service.base.thrifscala.ScrayTTypeInfo(
          scray.service.base.thrifscala.ScrayTType(2),
          Some("scray.classname")),
        ByteBuffer.allocate(10))),
    queryExpression = expr)
}

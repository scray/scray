package scray.core.service

import com.twitter.finagle.Thrift
import com.twitter.util.Await

object ScrayTServer {

  def main(args : Array[String]) {
    val server = Thrift.serveIface(scray.core.service.ENDPOINT, ScrayTServiceDummyImpl)
    Await.ready(server)
  }

}
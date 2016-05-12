package scray.example.adapter

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import java.net.URLConnection
import java.nio.CharBuffer

import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.LinkedBlockingQueue
import scala.util.parsing.combinator._
import java.util.concurrent.BlockingQueue



class YahooStreamAdapter(val outputQueue: BlockingQueue[Share]) extends Thread with LazyLogging {
  val url = "http://streamerapi.finance.yahoo.com/streamer/1.0?s=%5EGDAXI,USD=X&o=DBK.F,ADS.F,BAS.F,EOAN.F,SAP.F&k=l10,a00,b00,g00,h00&j=l10,a00,b00,g00,h00&r=0&marketid=us_market&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb";

  override def run {
    val client: HttpClient = new DefaultHttpClient();

    val response: HttpResponse = client.execute(new HttpGet(url));
    var rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

    val buffer = new Array[Char](10000);
    val parser = new YahooStockStreamParser

    while (true) {
      var dataSize = rd.read(buffer)
      val receivedData = CharBuffer.wrap(buffer, 0, dataSize)
      logger.debug(s"Received streaming data ${receivedData}")
      parser.parseAll(parser.start, receivedData) match {
        case parser.Success(share, _) => share.map { x => x.foreach { x =>  outputQueue.put(x)}}
        case parser.Failure(error, input) => logger.warn(error)
        case parser.Error(error, input) =>  logger.warn(error)
      }
    }
  }
}


class YahooStockStreamParser extends RegexParsers {
  override val skipWhitespace = false
  override val whiteSpace = """[ \t]""".r

  def start: Parser[Option[List[Share]]] = HEADER.? ~ time.? ~ shareObject.* ^^ {
    case header ~ time ~ shareObject => { Some(shareObject) }
  }

  def shareObject = OBJECT_PREFIX ~> STRING ~ ":{" ~
    ("l10:" ~> STRING <~ ("," | "}}")).? ~
    ("a00:" ~> STRING <~ ("," | "}}")).? ~
    ("b00:" ~> STRING <~ ("," | "}}")).? ~
    ("g00:" ~> STRING <~ ("," | "}}")).? ~
    ("h00:" ~> STRING <~ "}}").? ~
    OBJECT_SUFIX ^^ {
      case name ~ _ ~ l10 ~
        a00 ~
        b00 ~
        g00 ~
        h00 ~
        _ => { new Share(name, toFloat(l10), toFloat(a00), toFloat(b00), toFloat(g00), toFloat(h00)) }
    }

  def toFloat(string: Option[String]): Option[Float] = {
    string.map { x => x.toFloat }
  }

  def time = "<script>try{parent.yfs_mktmcb({\"unixtime\":" ~ "[0-9]+".r ~ ",\"open\":" ~ "[0-9]+".r ~ ",\"close\":" ~ "[0-9]+".r ~ "});}catch(e){}</script>"

  lazy val HEADER = "<html><head><script type='text/javascript'> document.domain='finance.yahoo.com'; </script> </head><body></body>"
  lazy val OBJECT_PREFIX = "<script>try{parent.yfs_u1f({"
  lazy val OBJECT_SUFIX = ");}catch(e){}</script>"
  lazy val STRING: Parser[String] = "\"" ~> ("\"\"" | "[^\"]".r).* <~ "\"" ^^ {
    case string => string mkString ("") replaceAll ("\"\"", "\"")
  }
}


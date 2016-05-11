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

class YahooStreamAdapter {

  def getStream = {
    val url: URL = new URL("http://streamerapi.finance.yahoo.com/streamer/1.0?s=^GDAXI,USD=X&o=DBK.F,ADS.F,BAS.F,EOAN.F,SAP.F&k=l10,a00,b00,g00,h00&j=l10,a00,b00,g00,h00&r=0&marketid=us_market&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb");

    val connection: URLConnection = url.openConnection();
    val streamReader: InputStreamReader = new InputStreamReader(connection.getInputStream(), "utf-8");

    streamReader
  }

  def getStream2 = {
    val url = "http://streamerapi.finance.yahoo.com/streamer/1.0?s=%5EGDAXI,USD=X&o=DBK.F,ADS.F,BAS.F,EOAN.F,SAP.F&k=l10,a00,b00,g00,h00&j=l10,a00,b00,g00,h00&r=0&marketid=us_market&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb";

    val client: HttpClient  = new DefaultHttpClient();
    val method: HttpGet = new HttpGet(url);
    
    val response: HttpResponse = client.execute(method);
    var rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

    var line = "";
    val buffer = new Array[Char](10000);

    val parser = new YahooStockStreamParser

    while(true) {
      var dataSize = rd.read(buffer)
      
      val string = CharBuffer.wrap(buffer, 0, dataSize)
      println(parser.parseAll(parser.start, string))
    }


  }
      class YahooStockStreamParser extends RegexParsers {
        override val skipWhitespace = false
        override val whiteSpace = """[ \t]""".r

        def start: Parser[Option[List[Share]]] = HEADER.? ~ time.? ~ shareObject.* ^^ {
          case header ~ time ~ shareObject => { Some(shareObject) }
        }

        def shareObject = OBJECT_PREFIX ~ STRING ~ ":{" ~
          ("l10:" <~ STRING).? ~
          (",a00:" <~ STRING).? ~
          (",b00:" <~ STRING).? ~
          (",g00:" <~ STRING).? ~
          (",h00:" <~ STRING).? ~
          "}}" ~
          OBJECT_SUFIX ^^ {
          case  _ ~ name ~ _ ~ 
          l10 ~
          a00 ~
          b00 ~
          g00 ~
          h00 
          ~ _ ~ _  => {new Share(name,  `=>F`(l10) ,`=>F`(a00), `=>F`(b00), `=>F`(g00), `=>F`(h00))}
          }
        
        def `=>F` (string: Option[String]): Option[Float] = {
          string.map { x => x.toFloat }
        }
        
        def time = "<script>try{parent.yfs_mktmcb({\"unixtime\":" ~ "[0-9]+".r ~ ",\"open\":" ~ "[0-9]+".r ~ ",\"close\":" ~ "[0-9]+".r ~ "});}catch(e){}</script>"

        lazy val HEADER = "<html><head><script type='text/javascript'> document.domain='finance.yahoo.com'; </script> </head><body></body>"
        lazy val TIME = "<script>try{parent.yfs_mktmcb({\"unixtime\":1462965657,\"open\":1462973400,\"close\":1462996800});}catch(e){}</script>"
        lazy val OBJECT_PREFIX = "<script>try{parent.yfs_u1f({"
        lazy val OBJECT_SUFIX = ");}catch(e){}</script>"
        lazy val STRING: Parser[String] = "\"" ~> ("\"\"" | "[^\"]".r).* <~ "\"" ^^ {
          case string => string mkString ("") replaceAll ("\"\"", "\"")
        }
      }
}

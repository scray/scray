package scray.example.adapter

import java.io.InputStreamReader
import java.net.URL
import java.net.URLConnection

import org.apache.http.NameValuePair
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import java.io.BufferedReader
import org.apache.http.HttpResponse
import scala.util.matching.Regex

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
    while(true) {
      var dataSize = rd.read(buffer)
      val string =  new String(buffer)
      
      val pattern = new Regex("""\(.*?\)""")
      
      println(string)
      val fffd = (pattern findAllIn string)
      fffd.map { x => println("AAAAAAAAAAAAAa" + x) }     
    }


  }

  def pars(c: Char, state: TokenizerState.Value): (TokenizerState.Value, Char) = {
    import scray.example.adapter.TokenizerState._
    state match {
      case WAIT_FOR_OBJECT => {
        if (c == '(') {
          (OBJECT_START_1, c)
        } else {
          (WAIT_FOR_OBJECT, c)
        }
      }
      case OBJECT_START_1 => {
        if (c == '{') {
          (COLLECT_OBJECT_DATA, c)
        } else {
          (WAIT_FOR_OBJECT, c)
        }
      }
      case COLLECT_OBJECT_DATA => {
        if (c == '}') {
          (OBJECT_END_1, c)
        } else {
          (COLLECT_OBJECT_DATA, c)
        }
      }
      case OBJECT_END_1 => {
        if (c == '}') {
          (OBJECT_END_2, c)
        } else {
          (COLLECT_OBJECT_DATA, c)
        }
      }
      case OBJECT_END_2 => {
        (WAIT_FOR_OBJECT, c)
      }
    }
    // val add = "abc<def>ffa".map { x => findStart(x) }.flatten.map { x => println("////////////// \t" + x) }
  }
}

object TokenizerState extends Enumeration {
  val OBJECT_START_1, OBJECT_END_1, OBJECT_END_2, COLLECT_OBJECT_DATA, WAIT_FOR_OBJECT = Value
}

package scray.example.adapter

import java.net.URL
import java.net.URLConnection
import java.io.InputStreamReader

class YahooStreamAdapter {
  
  def getStream = {
    val url: URL = new URL("http://streamerapi.finance.yahoo.com/streamer/1.0?s=^GDAXI,USD=X&o=DBK.F,ADS.F,BAS.F,EOAN.F,SAP.F&k=l10,a00,b00,g00,h00&j=l10,a00,b00,g00,h00&r=0&marketid=us_market&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb");   


		val connection: URLConnection = url.openConnection();
		val streamReader: InputStreamReader = new InputStreamReader(connection.getInputStream(), "utf-8");
		
		streamReader
  }
  

  def pars(c: Char, state: TokenizerState.Value): (TokenizerState.Value, Char) = {
    import scray.example.adapter.TokenizerState._
    state match { 
       case WAIT_FOR_OBJECT => {
         if(c == '(') {
           (OBJECT_START_1, c)
         } else {
           (WAIT_FOR_OBJECT, c)
         }
       }
       case OBJECT_START_1 => {
         if(c == '{') {
           (COLLECT_OBJECT_DATA, c)
         } else {
           (WAIT_FOR_OBJECT, c)
         }
       }
       case COLLECT_OBJECT_DATA => {
         if(c == '}') {
           (OBJECT_END_1, c)
         } else {
           (COLLECT_OBJECT_DATA, c)
         }
       }
       case  OBJECT_END_1 => {
         if(c == '}') {
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

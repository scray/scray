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
  

  def pars(c: Char, state: TokenizerState1.Value) {
     var open = false
      var string = ""
     
       object TokenizerState1 extends Enumeration {
        type TokenizerState1= Value
        val OBJECT_START_1 , OBJECT_START_2, OBJECT_END_1, OBJECT_END_2, COLLECT_OBJECT_DATA, WAIT_FOR_OBJECT = Value
      }
     
     val fff = TokenizerState1.COLLECT_OBJECT_DATA
      fff match { case TokenizerState1.COLLECT_OBJECT_DATA => "a" }
      def findStart(a: Char): Option[Char] = {

        if(a == '<') {
          open = true;
        } 
        
        if(a == '>' ) {
          open = false
        }
        
        if(open) {
          if(a == '<') {
            None
          } else {
            Some(a)
          }
        } else {
          None
        }
      }

      val add = "abc<def>ffa".map { x => findStart(x) }.flatten.map { x => println("////////////// \t" + x) }
  }
}

  object TokenizerState1 extends Enumeration {
    type TokenizerState1= Value
    val OBJECT_START_1 , OBJECT_START_2, OBJECT_END_1, OBJECT_END_2, COLLECT_OBJECT_DATA, WAIT_FOR_OBJECT = Value
  }
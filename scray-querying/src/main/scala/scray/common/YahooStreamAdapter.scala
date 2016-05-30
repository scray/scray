package scray.common

import java.net.URL
import java.net.URLConnection
import java.io.InputStreamReader

class YahooStreamAdapter {

  def getStream: InputStreamReader = {
    val url: URL = new URL("http://streamerapi.finance.yahoo.com/streamer/1.0?s=" + 
        "^GDAXI,USD=X&o=DBK.F,ADS.F,BAS.F,EOAN.F,SAP.F&k=l10,a00,b00,g00,h00&j=l10,a00,b00,g00,h00&r=0&marketid=us_market" + 
        "&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb");

    val connection: URLConnection = url.openConnection();
    val streamReader: InputStreamReader = new InputStreamReader(connection.getInputStream(), "utf-8");

    streamReader
  }
}

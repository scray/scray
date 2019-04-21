package org.scray

import org.openapitools.client.ApiInvoker

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val ff = new ApiInvoker();
    val client = ff.getClient("localhost")
    
  }

}

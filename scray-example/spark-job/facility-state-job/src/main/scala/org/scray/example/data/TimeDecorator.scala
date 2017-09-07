package org.scray.example.data

class TimeDecorator[T](requestTime: Long = System.currentTimeMillis(), data: T) extends Serializable {
  
  def getRequestTime: Long = {
    requestTime
  }
  
  def getData = {
    data
  }
}
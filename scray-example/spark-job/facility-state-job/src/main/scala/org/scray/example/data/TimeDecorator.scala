package org.scray.example.data

class TimeDecorator[T](requestTime: Long, data: T) {
  
  def getRequestTime: Long = {
    requestTime
  }
  
  def getData = {
    data
  }
}
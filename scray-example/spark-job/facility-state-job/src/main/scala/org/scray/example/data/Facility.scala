package org.scray.example.data

case class Facility[T](facilitytype: String, state: String, timestamp: T)

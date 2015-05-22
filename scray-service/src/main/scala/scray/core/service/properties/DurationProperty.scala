package scray.core.service.properties

import com.twitter.util.Duration
import java.util.concurrent.TimeUnit

import scray.common.properties.Property

class DurationProperty(name: String, defaultValue: Duration)
  extends Property[Long, Duration] {

  val DEFAULT_TIME_UNIT = TimeUnit.NANOSECONDS
  val DELIMITER = " "

  override def hasDefault(): Boolean = defaultValue != null
  override def getDefault(): Duration = defaultValue;
  override def getName(): String = name

  override def transformToResult(value: Long): Duration = Duration(value, DEFAULT_TIME_UNIT)
  override def transformToStorageFormat(value: Duration): Long = value.inUnit(DEFAULT_TIME_UNIT)

  override def toString(value: Long): String = value + DELIMITER + DEFAULT_TIME_UNIT.toString
  override def fromString(value: String): Long = {
    val arr = value.split(DELIMITER)
    Duration(arr(0).toInt, TimeUnit.valueOf(arr(1))).inUnit(DEFAULT_TIME_UNIT).toLong
  }

}

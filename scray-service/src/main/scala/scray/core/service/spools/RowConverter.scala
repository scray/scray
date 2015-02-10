package scray.core.service.spools
import com.twitter.bijection.Conversion.asMethod
import scray.querying.description.EmptyRow
import java.nio.ByteBuffer
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.querying.description.Row
import scray.common.serialization.KryoPoolSerialization
import scray.service.qmodel.thrifscala.ScrayTColumn
import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import com.twitter.chill.KryoInjection
import com.twitter.bijection.Injection
import com.twitter.bijection.Bijection
import com.twitter.bijection.GZippedBytes
import org.xerial.snappy.Snappy
import scray.common.properties.ScrayProperties
import scray.common.properties.predefined.PredefinedProperties

/**
 * Marker row demarcating the end of the result set (within a page)
 */
class SucceedingRow extends EmptyRow

/**
 * Utility function for converting rows between query model and service model including serialization
 */
object RowConverter {
  lazy val compressionSizeMinLength : Int = ScrayProperties.getPropertyValue(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE.getName())

  def convertRow(sRow : Row) : ScrayTRow = sRow match {
    case sRow : SucceedingRow => ScrayTRow(None, None)
    case _ => ScrayTRow(None, Some(sRow.getColumns.map { col =>
      ScrayTColumn(ScrayTColumnInfo(col.columnName, None, None), encode(sRow.getColumnValue(col).get))
    }))
  }

  val bytes2SnappyBytes = Bijection.build[Array[Byte], Array[Byte]](Snappy.compress(_))(Snappy.uncompress(_))
  val kryoCodec = KryoInjection.instance(KryoPoolSerialization.chill) // andThen bytes2SnappyBytes andThen Bijection.bytes2Buffer

  private def encode[V](value : V) : ByteBuffer = {
    val serial = kryoCodec(value)
    Bijection.bytes2Buffer(
      if (serial.length >= compressionSizeMinLength) bytes2SnappyBytes(serial) else serial)
  }
}
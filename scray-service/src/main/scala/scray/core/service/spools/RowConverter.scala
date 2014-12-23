package scray.core.service.spools

import scray.querying.description.EmptyRow
import java.nio.ByteBuffer
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.querying.description.Row
import scray.common.serialization.KryoPoolSerialization
import scray.service.qmodel.thrifscala.ScrayTColumn
import scray.service.qmodel.thrifscala.ScrayTColumnInfo

/**
 * Marker row demarcating the end of the result set (within a page)
 */
class SucceedingRow extends EmptyRow

/**
 * Utility function for converting rows between query model and service model including serialization
 */
object RowConverter {

  def convertRow(sRow : Row) : ScrayTRow = sRow match {
    case sRow : SucceedingRow => ScrayTRow(None, None)
    case _ => ScrayTRow(None, Some(sRow.getColumns.map { col =>
      ScrayTColumn(ScrayTColumnInfo(col.columnName, None, None), encode(sRow.getColumnValue(col).get))
    }))
  }

  private def encode[V](value : V) : ByteBuffer =
    ByteBuffer.wrap(KryoPoolSerialization.chill.toBytesWithClass(value))

}
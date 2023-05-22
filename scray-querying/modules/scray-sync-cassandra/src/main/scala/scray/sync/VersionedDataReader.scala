package scray.sync

import scala.util.Try
import scray.sync.api.VersionedData
import scala.util.Failure

class VersionedDataReader {
  def get(): Try[Iterator[VersionedData]] = {
    Try((List.empty[VersionedData].iterator))
  }
}
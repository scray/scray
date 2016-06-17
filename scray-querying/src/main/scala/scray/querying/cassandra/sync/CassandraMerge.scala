//package scray.querying.sync.cassandra
//
//import scray.querying.sync.MergeApi
//import scray.querying.sync.types.MonoidF
//import scala.util.Try
//import scray.querying.sync.types.RowWithValue
//
//class CassandraMerge extends MergeApi {
//  def merge(onlineData: RowWithValue, function: MonoidF[RowWithValue], batchData: RowWithValue): Try[Unit] = {
//    onlineData.foldLeft("")((A,B) => B)
//    Try()
//  }
//}
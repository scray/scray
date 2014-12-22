package scray.core.service.util

import scray.querying.description.SimpleRow
import scray.querying.description.Column
import com.twitter.util.Future
import scala.collection.mutable.ArrayBuffer
import com.twitter.concurrent.Spool
import scray.querying.description.RowColumn
import scray.querying.description.Row
import scray.querying.description.TableIdentifier
import com.twitter.concurrent.Spool.syntax1
import scala.util.Random

trait SpoolSamples {
  val ti = TableIdentifier("cassandra", "mytestspace", "mycf")

  val cols = 1.until(10).map(i => Column(s"col$i", ti))

  val sr1 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 34), RowColumn(cols(1), 21)))
  val sr2 = SimpleRow(ArrayBuffer(RowColumn(cols(1), 12), RowColumn(cols(2), "foo")))
  val sr3 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 56), RowColumn(cols(1), 34), RowColumn(cols(2), 456)))
  val sr4 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 1), RowColumn(cols(1), 34.4f)))
  val sr5 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 33), RowColumn(cols(1), 21)))
  val sr6 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 34), RowColumn(cols(1), "bar")))
  val sr7 = SimpleRow(ArrayBuffer())
  val sr8 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 100), RowColumn(cols(1), "baz")))

  // these seqs are ordered
  val seq1 = Future(Seq(sr4, sr1, sr3))
  val seq2 = Future(Seq(sr4, sr5, sr8))
  val seq3 = Future(Seq(sr8, sr7))
  val seq4 = Future(Seq(sr8, sr2))

  val spoolOf8 = Future(sr1 **:: sr2 **:: sr3 **:: sr4 **:: sr5 **:: sr6 **:: sr7 **:: sr8 **:: Spool.empty[Row])

  // these spools are ordered
  val spool1 = Future(sr4 **:: sr1 **:: sr3 **:: Spool.empty[Row])
  val spool2 = Future(sr4 **:: sr5 **:: sr8 **:: Spool.empty[Row])
  val spool3 = Future(sr8 **:: sr7 **:: Spool.empty[Row])
  val spool4 = Future(sr8 **:: sr2 **:: Spool.empty[Row])

  // spool generator
  def spoolGen(rows : Int, maxCols : Int, vals : Array[_]) : Future[Spool[Row]] = {
    val cols = 1.until(maxCols).map(i => Column(s"col$i", ti))
    Future(1.until(rows)
      .foldRight(Spool.empty[Row])((int, spl) => SimpleRow(1.until(Random.nextInt(maxCols))
        .map(col => RowColumn(cols(col), vals(Random.nextInt(vals.length - 1))))
        .foldLeft(ArrayBuffer[RowColumn[_]]())((abuf, rcol) => abuf += rcol)) **:: spl))
  }

}
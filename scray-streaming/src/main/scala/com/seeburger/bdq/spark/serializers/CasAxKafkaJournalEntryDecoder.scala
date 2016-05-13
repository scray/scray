package com.seeburger.bdq.spark.serializers

import kafka.serializer.Decoder
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import kafka.utils.VerifiableProperties

class CasAxKafkaJournalEntryDecoder(config: VerifiableProperties) extends Decoder[CasAxKafkaJournalEntry] {
  def this() = this(null)
  override def fromBytes(bytes: Array[Byte]): CasAxKafkaJournalEntry = {
    val bais = new ByteArrayInputStream(bytes)
    val in = new DataInputStream(bais)
    val table = in.readUTF()
    val msgId = in.readUTF()
    val time = in.readLong()
    val delete = in.readBoolean()
    in.close()
    CasAxKafkaJournalEntry(table, msgId, time, delete)
  }
}
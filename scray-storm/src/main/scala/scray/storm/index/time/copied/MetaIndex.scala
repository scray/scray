package scray.storm.index.time.copied

object IndexKind extends Enumeration {
  type Kind = Value;
  val TimeIndex, WildcardIndex = Value;
}

object IndexPurpose extends Enumeration {
  type Purpose = Value;
  val Requirement, Customization, Test, Unspecified = Value;
}

object IndexStatus extends Enumeration {
  type Status = Value;
  val Planned, Experimental, Implemented, Production = Value;
}

trait IndexMemberBase {
  val identifier: String;
  val table: String;
  val column: String;
  val queryGroup: String;
  val description: String;
  val purpose: IndexPurpose.Purpose = IndexPurpose.Unspecified;
  val status: IndexStatus.Status = IndexStatus.Planned;
  // val runner: InitiableRunner[_, _, _];
}

sealed trait IndexMember extends IndexMemberBase {
  val kind: IndexKind.Kind;
}

trait TimeIndex extends IndexMember {
  val kind = IndexKind.TimeIndex;
}

trait WildcardIndex extends IndexMember {
  val kind = IndexKind.WildcardIndex;
}

/**
 * An abstract enumeration of index cases
 */
abstract class MetaIndex extends Enumeration {

  // Index Values
  class IndexVal(
    override val identifier: String,
    override val table: String,
    override val column: String,
    override val queryGroup: String,
    override val description: String,
    override val purpose: IndexPurpose.Purpose,
    override val status: IndexStatus.Status)
    // override val runner: InitiableRunner[_, _, _])
    extends Val(identifier) with IndexMemberBase;

  // we'd like to use Enumeration Vals as IndexMembers
  implicit def valueToIndex(v: Value): IndexMember = v.asInstanceOf[IndexMember];
}

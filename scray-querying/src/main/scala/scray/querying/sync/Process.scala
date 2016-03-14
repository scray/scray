//package scray.querying.sync
//
//class Process {
//
//}
//
//object SyncTableFSM {
//
//  sealed trait State
//  final class MASTER_INITIALIZED extends State
//  final class WORKER_INITIALIZED extends State
//  final class RUNNING extends State
//  final class FINISHED extends State
//
//  sealed trait Message
//  case class Message1(s: String) extends Message
//  case class Message2(s: String) extends Message
//
//  sealed trait ResultMessage
//  object ResultMessage1 extends ResultMessage
//  object ResultMessage2 extends ResultMessage
//}
//
//import TypicalFSM._
//
//case class Transformation[M <: Message, From <: State, To <: State](
//    f: M => Seq[ResultMessage]) {
//
//  def apply(m: M) = f(m)
//}
//
//object Transformation {
//
//  implicit def `message1 in state1` =
//    Transformation[Message1, MASTER_INITIALIZED, WORKER_INITIALIZED] { m =>
//
//      Seq(ResultMessage1, ResultMessage2)
//    }
//
//  implicit def `message1 in state2` =
//    Transformation[Message1, State2, State2] { m =>
//      Seq(ResultMessage1)
//    }
//
//  implicit def `message2 in state2` =
//    Transformation[Message2, State2, State1] { m =>
//      Seq(ResultMessage2)
//    }
//}
//
//class TypicalFSM[CurrentState <: State] {
//
//  def apply[M <: Message, NewState <: State](message: M)(
//    implicit transformWith: Transformation[M, CurrentState, NewState]) = {
//
//    this.asInstanceOf[TypicalFSM[NewState]] -> transformWith(message)
//  }
//}
/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed
package internal

import akka.{ actor ⇒ a }
import akka.dispatch.sysmsg._
import akka.util.Unsafe.{ instance ⇒ unsafe }
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * Every ActorRef is also an ActorRefImpl, but these two methods shall be
 * completely hidden from client code. There is an implicit converter
 * available in the package object, enabling `ref.toImpl` (or `ref.toImplN`
 * for `ActorRef[Nothing]`—Scala refuses to infer `Nothing` as a type parameter).
 */
private[typed] trait ActorRefImpl[-T] extends ActorRef[T] {
  def sendSystem(signal: SystemMessage): Unit
  def isLocal: Boolean
}

/**
 * A local ActorRef that is backed by an asynchronous [[ActorCell]].
 */
private[typed] class LocalActorRef[T](_path: a.ActorPath, private[typed] val cell: ActorCell[T])
  extends ActorRef[T](_path) with ActorRefImpl[T] {
  override def tell(msg: T): Unit = cell.send(msg)
  override def sendSystem(signal: SystemMessage): Unit = cell.sendSystem(signal)
  final override def isLocal: Boolean = true
}

/**
 * A local synchronous ActorRef that invokes the given function for every message send.
 * This reference can be watched and will do the right thing when it receives a [[DeathWatchNotification]].
 * This reference cannot watch other references.
 */
private[typed] final class FunctionRef[-T](_path: a.ActorPath, override val isLocal: Boolean,
                                           send: (T, FunctionRef[T]) ⇒ Unit, terminate: FunctionRef[T] ⇒ Unit)
  extends ActorRef[T](_path) with ActorRefImpl[T] {

  override def tell(msg: T): Unit =
    if (_watchedBy != null) // we don’t have deadLetters available
      try send(msg, this) catch {
        case NonFatal(ex) ⇒ // nothing we can do here
      }

  override def sendSystem(signal: SystemMessage): Unit = signal match {
    case Create()                           ⇒ // nothing to do
    case DeathWatchNotification(ref, cause) ⇒ // we’re not watching, and we’re not a parent either
    case Terminate()                        ⇒ doTerminate()
    case Watch(watchee, watcher)            ⇒ if (watchee == this && watcher != this) addWatcher(watcher.toImplN)
    case Unwatch(watchee, watcher)          ⇒ if (watchee == this && watcher != this) remWatcher(watcher.toImplN)
    case NoMessage                          ⇒ // nothing to do
  }

  def stop(): Unit = doTerminate()

  /*
   * Private Implementation
   */
  import FunctionRef._

  type S = Set[ActorRefImpl[Nothing]]
  @volatile private[this] var _watchedBy: S = Set.empty

  protected def doTerminate(): Unit = {
    val watchedBy = unsafe.getAndSetObject(this, watchedByOffset, null).asInstanceOf[S]
    if (watchedBy != null) {
      try terminate(this) catch { case NonFatal(ex) ⇒ }
      if (watchedBy.nonEmpty) watchedBy foreach sendTerminated
    }
  }

  private def sendTerminated(watcher: ActorRefImpl[Nothing]): Unit =
    watcher.sendSystem(DeathWatchNotification(this, null))

  @tailrec private def addWatcher(watcher: ActorRefImpl[Nothing]): Unit =
    _watchedBy match {
      case null ⇒ sendTerminated(watcher)
      case watchedBy ⇒
        if (!watchedBy.contains(watcher))
          if (!unsafe.compareAndSwapObject(this, watchedByOffset, watchedBy, watchedBy + watcher))
            addWatcher(watcher) // try again
    }

  @tailrec private def remWatcher(watcher: ActorRefImpl[Nothing]): Unit = {
    _watchedBy match {
      case null ⇒ // do nothing...
      case watchedBy ⇒
        if (watchedBy.contains(watcher))
          if (!unsafe.compareAndSwapObject(this, watchedByOffset, watchedBy, watchedBy - watcher))
            remWatcher(watcher) // try again
    }
  }
}

private[typed] object FunctionRef {
  val watchedByOffset = unsafe.objectFieldOffset(classOf[FunctionRef[_]].getDeclaredField("_watchedBy"))
}

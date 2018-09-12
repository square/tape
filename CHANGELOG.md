Change Log
==========

Version 2.0.0-beta1 *(2018-09-12)*
----------------------------------

Tape 2 is a major release that focusses on simplifying the API. Tape 2 also adopts a new file format that allows QueueFile to grow beyond 4GB.

  * New: `remove(int)` allows atomic removal of multiple elements.
  * Improvement: `QueueFile` now implements the `Closeable` and `Iterable` interface.
  * Improvement: Simplified `ObjectQueue` API. `ObjectQueue` now implements the `Closeable` and `Iterable` interface. `ObjectQueue.Listener` has been removed.
  * New: Iterator API allows iterating queue items and stopping iteration early.
  * Improvement: New file format that allows QueueFile to grow beyond 4GB. Tape 2 can continue to operate on the v1 format. You can also force the legacy format by using the `forceLegacy` option.

Note: Existing queues are currently not migrated to the newer format. This will be added in a future release.

Version 1.2.3 *(2014-10-07)*
----------------------------

 * `close()` method on `FileObjectQueue` now closes underlying `QueueFile`.
 * Fix: Properly move and zero out bytes during copy opertaions.


Version 1.2.2 *(2014-03-18)*
----------------------------

 * Fix: Prevent corruption when expanding a perfectly saturated queue.


Version 1.2.1 *(2014-03-06)*
----------------------------

 * New: `QueueFile` instances throw an `IOException` on creation if a corrupt header is found.


Version 1.2.0 *(2014-01-16)*
----------------------------

 * New: Zero out old records in `clear()` and `remove()`.


Version 1.1.1 *(2013-10-21)*
----------------------------

 * Fix: Ensure -1 is returned when no more bytes are available.
 * Fix: Correct read to use position offset in certain cases where it would otherwise use 0.


Version 1.1.0 *(2012-11-08)*
----------------------------

 * Allow a `TaskQueue`'s `TaskInjector` to be `null`.
 * `TaskQueue` listener is now called with a reference to the queue instance rather than its delegate.


Version 1.0.0 *(2012-09-25)*
----------------------------

Initial release.

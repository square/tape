Change Log
==========

Version 1.2.4-41c3b39 *(2020-10-28)*
----------------------------

 * `checkQueueIntegrity()` method on `FileObjectQueue` and `QueueFile`

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

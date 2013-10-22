Change Log
==========

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

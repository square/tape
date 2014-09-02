Tape by Square, Inc.
====================

Tape is a collection of queue-related classes for Android and Java.

`QueueFile` is a lightning-fast, transactional, file-based FIFO. Addition and
removal from an instance is an O(1) operation and is atomic. Writes are
synchronous; data will be written to disk before an operation returns. The
underlying file is structured to survive process and even system crashes and if
an I/O exception is thrown during a mutating change, the change is aborted.

**NOTE:** The current implementation is built
for file systems that support atomic segment writes (like YAFFS). Most
conventional file systems don't support this; if the power goes out while
writing a segment, the segment will contain garbage and the file will be
corrupt. We'll add journaling support so this class can be used with more
file systems later.

An `ObjectQueue` represents an ordering of arbitrary objects which can be backed
either by the filesystem (via `QueueFile`) or in memory only.

`TaskQueue` is a special object queue which holds `Task`s, objects which have a
notion of being executed. Instances are managed by an external executor which
prepares and executes enqueued tasks.

*Some examples are available on [the website][1].*



Download
--------

Downloadable .jars can be found on the [GitHub download page][2].

You can also depend on the .jar through Maven:

```xml
<dependency>
    <groupId>com.squareup</groupId>
    <artifactId>tape</artifactId>
    <version>(insert latest version)</version>
</dependency>
```



License
-------

    Copyright 2012 Square, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.



 [1]: http://square.github.com/tape/
 [2]: http://github.com/square/tape/downloads

Tape by Square, Inc.
====================

Tape is a collection of queue-related classes for Android and Java.

`QueueFile` is a lightning-fast, transactional, file-based FIFO. Addition and
removal from an instance is an O(1) operation and is atomic. Writes are
synchronous; data will be written to disk before an operation returns. The
underlying file is structured to survive process and even system crashes and if
an I/O exception is thrown during a mutating change, the change is aborted.

An `ObjectQueue` represents an ordering of arbitrary objects which can be backed
either by the filesystem (via `QueueFile`) or in memory only.

`TaskQueue` is a special object queue which holds `Task`s, objects which have a
notion of being executed. Instances are managed by an external executor which
prepares and executes enqueued tasks.

*Some examples are available on [the website][1].*



Download
--------

Download [the latest JAR][2] or grab via Maven:
```xml
<dependency>
  <groupId>com.squareup</groupId>
  <artifactId>tape</artifactId>
  <version>1.2.2</version>
</dependency>
```
or Gradle:
```groovy
compile 'com.squareup:tape:1.2.2'
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
 [2]: http://repository.sonatype.org/service/local/artifact/maven/redirect?r=central-proxy&g=com.squareup&a=tape&v=LATEST

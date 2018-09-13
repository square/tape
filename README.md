Tape by Square, Inc.
====================

Tape is a collection of queue-related classes for Android and Java.

`QueueFile` is a lightning-fast, transactional, file-based FIFO. Addition and
removal from an instance is an O(1) operation and is atomic. Writes are
synchronous; data will be written to disk before an operation returns. The
underlying file is structured to survive process and even system crashes and if
an I/O exception is thrown during a mutating change, the change is aborted.

**NOTE:** The current implementation is built for file systems that support
atomic segment writes (like YAFFS). Most conventional file systems don't support
this; if the power goes out while writing a segment, the segment will contain
garbage and the file will be corrupt.

An `ObjectQueue` represents an ordering of arbitrary objects which can be backed
either by the filesystem (via `QueueFile`) or in memory only.



Download
--------

Download [the latest JAR][2] or grab via Maven:
```xml
<dependency>
  <groupId>com.squareup.tape2</groupId>
  <artifactId>tape</artifactId>
  <version>2.0.0-beta1</version>
</dependency>
```
or Gradle:
```groovy
compile 'com.squareup.tape2:tape:2.0.0-beta1'
```

Snapshots of the development version are available in [Sonatype's `snapshots` repository][snap].



Usage
-----

Create a `QueueFile` instance.

```java
File file = // ...
QueueFile queueFile = new QueueFile.Builder(file).build();
```

Add some data to the queue to the end of the queue. `QueueFile` accepts a `byte[]` of arbitrary length.

```java
queueFile.add("data".getBytes());
```

Read data at the head of the queue.

```java
byte[] data = queueFile.peek();
```

Remove processed elements.

```java
// Remove the eldest element.
queueFile.remove();

// Remove multiple elements.
queueFile.remove(n);

// Remove all elements.
queueFile.clear();
```

Read and process multiple elements with the iterator API.

```java
Iterator<byte[]> iterator = queueFile.iterator();
while (iterator.hasNext()) {
  byte[] element = iterator.next();
  process(element);
  iterator.remove();
}
```

While `QueueFile` works with `byte[]`, `ObjectQueue` works with arbitrary Java objects with a similar API. An `ObjectQueue` may be backed by a persistent `QueueFile`, or in memory. A persistent `ObjectQueue` requires a [`Converter`](#converter) to encode and decode objects.

```java
// A persistent ObjectQueue.
ObjectQueue<String> queue = ObjectQueue.create(queueFile, converter);

// An in-memory ObjectQueue.
ObjectQueue<String> queue = ObjectQueue.createInMemory();
```

Add some data to the queue to the end of the queue.

```java
queue.add("data");
```

Read data at the head of the queue.

```java
// Peek the eldest elements.
String data = queue.peek();

// Peek the eldest `n` elements.
List<String> data = queue.peek(n);

// Peek all elements.
List<String> data = queue.asList();
```

Remove processed elements.

```java
// Remove the eldest element.
queue.remove();

// Remove multiple elements.
queue.remove(n);

// Remove all elements.
queue.clear();
```

Read and process multiple elements with the iterator API.

```java
Iterator<String> iterator = queue.iterator();
while (iterator.hasNext()) {
  String element = iterator.next();
  process(element);
  iterator.remove();
}
```



Converter
---------

A `Converter` encodes objects to bytes and decodes objects from bytes.

```java
/** Converter which uses Moshi to serialize instances of class T to disk. */
class MoshiConverter<T> implements Converter<T> {
  private final JsonAdapter<T> jsonAdapter;

  public MoshiConverter(Moshi moshi, Class<T> type) {
    this.jsonAdapter = moshi.adapter(type);
  }

  @Override public T from(byte[] bytes) throws IOException {
    return jsonAdapter.fromJson(new Buffer().write(bytes));
  }

  @Override public void toStream(T val, OutputStream os) throws IOException {
    try (BufferedSink sink = Okio.buffer(Okio.sink(os))) {
      jsonAdapter.toJson(sink, val);
    }
  }
}
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
 [2]: https://search.maven.org/remote_content?g=com.squareup&a=tape&v=LATEST
 [snap]: https://oss.sonatype.org/content/repositories/snapshots/

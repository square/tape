// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import okio.BufferedSink;
import okio.Okio;

import static org.junit.Assert.assertTrue;

final class QueueTestUtils {
  static final String TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE =
      "/truncated-one-entry-serialized-queue";
  static final String TRUNCATED_EMPTY_SERIALIZED_QUEUE = "/truncated-empty-serialized-queue";
  static final String ONE_ENTRY_SERIALIZED_QUEUE = "/one-entry-serialized-queue";
  static final String EMPTY_SERIALIZED_QUEUE = "/empty-serialized-queue";
  static final String FRESH_SERIALIZED_QUEUE = "/fresh-serialized-queue";

  static File copyTestFile(String file) throws IOException {
    File newFile = File.createTempFile(file, "test");
    InputStream in = QueueTestUtils.class.getResourceAsStream(file);
    try (BufferedSink sink = Okio.buffer(Okio.sink(newFile))) {
      sink.writeAll(Okio.source(in));
    }
    assertTrue(newFile.exists());
    return newFile;
  }

  /** File that suppresses deletion. */
  static class UndeletableFile extends File {
    private static final long serialVersionUID = 1L;

    UndeletableFile(String name) {
      super(name);
    }

    @Override public boolean delete() {
      return false;
    }
  }

  private QueueTestUtils() {
    throw new AssertionError("No instances.");
  }
}

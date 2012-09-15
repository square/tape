// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.File;
import java.io.IOException;

/** Encapsulates an {@link IOException} in an extension of {@link RuntimeException}. */
public class FileException extends RuntimeException {
  private final File file;

  public FileException(String message, IOException e, File file) {
    super(message, e);
    this.file = file;
  }

  public File getFile() {
    return file;
  }
}

// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Serialized object queue converter.
 *
 * <b>This class is not thread safe; instances should be kept thread-confined.</b>
 *
 * @param <T> Object type.
 */
public class SerializedConverter<T extends Serializable> implements FileObjectQueue.Converter<T> {
  /** Deserialize a stream to an object. */
  @SuppressWarnings("unchecked")
  private T deserialize(InputStream in) throws IOException {
    ObjectInputStream oin = new ObjectInputStream(new BufferedInputStream(in, 1024));
    T entry;
    try {
      entry = (T) oin.readUnshared();
    } catch (ClassNotFoundException e) {
      // This can only happen if we make an incompatible change.
      throw new AssertionError(e);
    }
    return entry;
  }

  /** Deserialize bytes to an object. */
  @Override public T from(byte[] bytes) throws IOException {
    return deserialize(new ByteArrayInputStream(bytes));
  }

  /** Serializes o to bytes. */
   @Override public void toStream(T o, OutputStream bytes) throws IOException {
    ObjectOutputStream out = new ObjectOutputStream(bytes);
    out.writeUnshared(o);
    out.close();
  }
}

package com.squareup.tape2;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This is a simple "NO-OP" converter for byte[] queues which don't actually need a conversion.
 */
public class ByteArrayConverter implements ObjectQueue.Converter<byte[]> {

  public static final ByteArrayConverter INSTANCE = new ByteArrayConverter();

  @Override public byte[] from(byte[] source) {
    return source;
  }

  @Override public void toStream(byte[] value, OutputStream sink) throws IOException {
    sink.write(value);
  }
}

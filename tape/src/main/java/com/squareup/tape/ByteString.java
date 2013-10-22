package com.squareup.tape;

import java.nio.charset.Charset;

public class ByteString {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  // TODO public static ByteString from(InputStream stream) { .. }

  public static ByteString from(String string) {
    return new ByteString(string.getBytes(UTF8));
  }

  public static ByteString from(byte[] bytes) {
    return new ByteString(bytes);
  }

  // TODO InputStream stream
  private byte[] bytes;

  ByteString(byte[] bytes) {
    this.bytes = bytes;
  }

  // TODO public InputStream asStream() { .. }

  public byte[] asBytes() {
    return bytes;
  }

  public String asString() {
    return new String(bytes, UTF8);
  }
}

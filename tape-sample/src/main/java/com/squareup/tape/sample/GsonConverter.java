// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

import com.google.gson.Gson;
import com.squareup.tape.FileObjectQueue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

/**
 * Use GSON to serialize classes to a bytes.
 * <p/>
 * Note: This will only work when concrete classes are specified for {@code T}. If you want to specify an interface for
 * {@code T} then you need to also include the concrete class name in the serialized byte array so that you can
 * deserialize to the appropriate type.
 */
public class GsonConverter<T> implements FileObjectQueue.Converter<T> {
  private final Gson gson;
  private final Class<T> type;

  public GsonConverter(Gson gson, Class<T> type) {
    this.gson = gson;
    this.type = type;
  }

  @Override public T from(byte[] bytes) {
    Reader reader = new InputStreamReader(new ByteArrayInputStream(bytes));
    return gson.fromJson(reader, type);
  }

  @Override public void toStream(T object, OutputStream bytes) throws IOException {
    Writer writer = new OutputStreamWriter(bytes);
    gson.toJson(object, writer);
    writer.close();
  }
}
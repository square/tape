package com.squareup.tape;

public interface FileQueue {
  void add(ByteString data);
  boolean isEmpty();
  void peek(ElementVisitor visitor);

  interface ElementVisitor {
    void visit(ElementControl control);
  }
  interface ElementControl {
    void remove();
  }
}

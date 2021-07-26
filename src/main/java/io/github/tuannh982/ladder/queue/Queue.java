package io.github.tuannh982.ladder.queue;

import java.io.Closeable;

public interface Queue extends Closeable {
    long put(byte[] value);
    byte[] take();
}

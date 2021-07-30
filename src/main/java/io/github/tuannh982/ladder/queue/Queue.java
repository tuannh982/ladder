package io.github.tuannh982.ladder.queue;

import java.io.Closeable;
import java.io.IOException;

public interface Queue extends Closeable {
    long put(byte[] value) throws IOException;
    byte[] take() throws IOException;
    String stats();
}

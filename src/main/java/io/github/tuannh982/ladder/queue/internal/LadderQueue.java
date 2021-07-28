package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.queue.Queue;

import java.io.File;
import java.io.IOException;

public class LadderQueue implements Queue {
    private final LadderQueueInternal internal;

    public LadderQueue(File dir, LadderQueueOptions options) throws IOException {
        if (!dir.isDirectory()) {
            throw new AssertionError(dir.getName() + " is not a directory");
        }
        internal = LadderQueueInternal.open(dir, options);
    }

    @Override
    public long put(byte[] value) throws IOException {
        return internal.put(value);
    }

    @Override
    public byte[] take() throws IOException {
        return internal.take();
    }

    @Override
    public void close() throws IOException {
        internal.close();
    }
}

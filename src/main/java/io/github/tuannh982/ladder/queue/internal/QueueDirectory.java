package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.commons.io.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class QueueDirectory implements Closeable {
    private final File dir;
    private final FileChannel dirChannel;
    private final Path path;

    public QueueDirectory(File dir) throws IOException {
        FileUtils.mkdir(dir);
        this.dir = dir;
        FileChannel channel = null;
        try {
            channel = FileChannel.open(dir.toPath(), StandardOpenOption.READ);
        } catch (Exception ignored) {
            // unnecessary to initiate directory channel
        }
        this.dirChannel = channel;
        this.path = dir.toPath();
    }

    public File[] dataFiles() {
        return DirectoryUtils.dataFiles(dir);
    }

    public Path path() {
        return path;
    }

    public File file() {
        return dir;
    }

    public void sync() throws IOException {
        if (dirChannel != null && dirChannel.isOpen()) {
            dirChannel.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        if (dirChannel != null && dirChannel.isOpen()) {
            dirChannel.force(true);
            dirChannel.close();
        }
    }
}

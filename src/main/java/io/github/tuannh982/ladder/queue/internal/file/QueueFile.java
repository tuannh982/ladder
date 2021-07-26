package io.github.tuannh982.ladder.queue.internal.file;

import io.github.tuannh982.ladder.commons.number.NumberUtils;
import io.github.tuannh982.ladder.queue.internal.LadderQueueOptions;
import io.github.tuannh982.ladder.queue.internal.QueueDirectory;
import io.github.tuannh982.ladder.queue.internal.Record;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Slf4j
@Getter
public class QueueFile implements Closeable {
    private static final String INDEX_FILE_EXTENSION = ".index";

    private final long startSequenceNumber;
    private final QueueDirectory queueDirectory;
    private final LadderQueueOptions options;
    //
    private final File file;
    private final FileChannel channel;
    //
    private int unflushed = 0;
    private int writeOffset = 0;

    private QueueFile(long startSequenceNumber, QueueDirectory queueDirectory, LadderQueueOptions options, File file, FileChannel channel) throws IOException {
        this.startSequenceNumber = startSequenceNumber;
        this.queueDirectory = queueDirectory;
        this.options = options;
        this.file = file;
        this.channel = channel;
        this.writeOffset = NumberUtils.checkedCast(channel.size());
    }

    public File file() {
        return file;
    }

    public Path path() {
        return file.toPath();
    }

    @SuppressWarnings("java:S2095")
    public static QueueFile create(long startSequenceNumber, QueueDirectory queueDirectory, LadderQueueOptions options) throws IOException {
        File file = queueDirectory.path().resolve(startSequenceNumber + INDEX_FILE_EXTENSION).toFile();
        boolean b = file.createNewFile();
        if (!b) {
            throw new IOException(file.getName() + " already existed");
        }
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        return new QueueFile(startSequenceNumber, queueDirectory, options, file, channel);
    }

    @SuppressWarnings("java:S2095")
    public static QueueFile open(long startSequenceNumber, QueueDirectory queueDirectory, LadderQueueOptions options) throws IOException {
        Path path = queueDirectory.path().resolve(startSequenceNumber + INDEX_FILE_EXTENSION);
        if (!Files.exists(path)) {
            throw new IOException(path.toString() + " did not exists");
        }
        File file = path.toFile();
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        return new QueueFile(startSequenceNumber, queueDirectory, options, file, channel);
    }

    public void flushToDisk() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
        }
    }

    public void flush() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(false); // no need to write metadata
        }
    }

    @SuppressWarnings({"java:S4042", "java:S899"})
    public void delete() {
        if (file != null) {
            file.delete();
        }
    }

    public void write(Record entry) throws IOException {
        ByteBuffer[] buffers = entry.serialize();
        long toBeWritten = 0;
        for (ByteBuffer buffer : buffers) {
            toBeWritten += buffer.remaining();
        }
        long written = 0;
        while (written < toBeWritten) {
            written += channel.write(buffers);
        }
        writeOffset += written;
        unflushed += written;
        if (unflushed > options.getDataFlushThreshold()) {
            flush();
            unflushed = 0;
        }
    }

    public Iterator<Record> iterator() throws IOException {
        return new QueueFileIterator();
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
    }

    private class QueueFileIterator implements Iterator<Record> {
        private final FileChannel iterChannel;
        private final long channelSize;
        private long offset = 0;

        public QueueFileIterator() throws IOException {
            iterChannel = FileChannel.open(path(), StandardOpenOption.READ);
            channelSize = iterChannel.size();
        }

        @Override
        public boolean hasNext() {
            return offset < channelSize;
        }

        @Override
        public Record next() {
            if (hasNext()) {
                try {
                    ByteBuffer headerBuffer = ByteBuffer.allocate(Record.HEADER_SIZE);
                    offset += channel.read(headerBuffer);
                    Record.Header header = Record.Header.deserialize(headerBuffer);
                    ByteBuffer dataBuffer = ByteBuffer.allocate(header.getValueSize());
                    offset += channel.read(dataBuffer);
                    Record entry = Record.deserialize(dataBuffer, header);
                    if (!entry.verifyChecksum()) {
                        throw new IOException("checksum failed");
                    }
                    return entry;
                } catch (IOException e) {
                    log.error("index file corrupted ", e);
                    offset = channelSize;
                }
            }
            try {
                if (iterChannel != null && iterChannel.isOpen()) {
                    iterChannel.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            throw new NoSuchElementException();
        }
    }
}

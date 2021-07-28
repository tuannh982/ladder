package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.commons.concurrent.RLock;
import io.github.tuannh982.ladder.queue.internal.file.QueueFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class LadderQueueInternal implements Closeable {
    private final LadderQueueOptions options;
    private final QueueDirectory queueDirectory;
    // data
    private final NavigableMap<Long, QueueFile> queueFileMap;
    // files
    private QueueFile currentQueueFile;
    // sequence number
    private long writeSequenceNumber;
    // locks
    private final RLock writeLock;

    public static LadderQueueInternal open(File dir, LadderQueueOptions options) throws IOException {
        QueueDirectory queueDirectory = new QueueDirectory(dir);
        Map.Entry<NavigableMap<Long, QueueFile>, Long> buildQueueFileMapReturn = DirectoryUtils.buildQueueFileMap(queueDirectory, options);
        NavigableMap<Long, QueueFile> queueFileMap = buildQueueFileMapReturn.getKey();
        long maxSequenceNumber = buildQueueFileMapReturn.getValue();
        if (maxSequenceNumber < 0) {
            maxSequenceNumber = 0;
        }
        return new LadderQueueInternal(
                options,
                queueDirectory,
                queueFileMap,
                maxSequenceNumber + 1
        );
    }

    private LadderQueueInternal(
            LadderQueueOptions options,
            QueueDirectory queueDirectory,
            NavigableMap<Long, QueueFile> queueFileMap,
            long writeSequenceNumber
            ) {
        this.options = options;
        this.queueDirectory = queueDirectory;
        this.queueFileMap = queueFileMap;
        this.writeSequenceNumber = writeSequenceNumber;
        //
        this.writeLock = new RLock();
    }

    public long put(byte[] value) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            Record entry = new Record(value);
            entry.getHeader().setSequenceNumber(nextSequenceNumber());
            writeToCurrentQueueFile(entry);
            return entry.getHeader().getSequenceNumber();
        } finally {
            writeLock.release(rlock);
        }
    }

    public byte[] take() throws IOException {
        // TODO
    }

    @Override
    public void close() throws IOException {
        boolean rlock = writeLock.lock();
        try {
            if (currentQueueFile != null) {
                currentQueueFile.close();
            }
            queueDirectory.close();
            // TODO save read metadata
            // TODO
        } finally {
            writeLock.release(rlock);
        }
    }

    private long nextSequenceNumber() {
        return writeSequenceNumber++;
    }

    private void writeToCurrentQueueFile(Record entry) throws IOException {
        rolloverCurrentQueueFile(entry);
        currentQueueFile.write(entry);
    }

    private void rolloverCurrentQueueFile(Record entry) throws IOException {
        if (currentQueueFile == null) {
            currentQueueFile = createNewQueueFile(entry.getHeader().getSequenceNumber());
            queueDirectory.sync();
        } else if (currentQueueFile.getWriteOffset() + entry.serializedSize() > options.getMaxFileSize()) {
            currentQueueFile.close();
            currentQueueFile = createNewQueueFile(entry.getHeader().getSequenceNumber());
            queueDirectory.sync();
        }
    }

    public QueueFile createNewQueueFile(long fileStartSequenceNumber) throws IOException {
        QueueFile file = QueueFile.create(fileStartSequenceNumber, queueDirectory, options);
        if (queueFileMap.putIfAbsent(file.getStartSequenceNumber(), file) != null) {
            throw new IOException("File already existed");
        }
        return file;
    }
}

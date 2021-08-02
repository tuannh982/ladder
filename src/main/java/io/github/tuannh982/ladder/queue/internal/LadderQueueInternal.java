package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.commons.concurrent.RLock;
import io.github.tuannh982.ladder.queue.internal.file.QueueFile;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class LadderQueueInternal implements Closeable {
    private final LadderQueueOptions options;
    private final QueueDirectory queueDirectory;
    // data
    private final NavigableMap<Long, QueueFile> queueFileMap;
    // files
    private QueueFile currentQueueFile;
    // sequence number
    private long writeSequenceNumber;
    private final ReadMetadata readMetadata;
    private QueueFile currentReadFile;
    // locks
    private final RLock writeLock;
    // task
    private final ExecutorService compactionService = Executors.newSingleThreadExecutor();

    public static LadderQueueInternal open(File dir, LadderQueueOptions options) throws IOException {
        QueueDirectory queueDirectory = new QueueDirectory(dir);
        ReadMetadata readMetadata = ReadMetadata.open(queueDirectory);
        Map.Entry<NavigableMap<Long, QueueFile>, Long> buildQueueFileMapReturn = DirectoryUtils.buildQueueFileMap(queueDirectory, readMetadata, options);
        NavigableMap<Long, QueueFile> queueFileMap = buildQueueFileMapReturn.getKey();
        long maxSequenceNumber = buildQueueFileMapReturn.getValue();
        if (maxSequenceNumber < 0) {
            maxSequenceNumber = 0;
        }
        return new LadderQueueInternal(
                options,
                queueDirectory,
                queueFileMap,
                maxSequenceNumber + 1,
                readMetadata);
    }

    private LadderQueueInternal(
            LadderQueueOptions options,
            QueueDirectory queueDirectory,
            NavigableMap<Long, QueueFile> queueFileMap,
            long writeSequenceNumber,
            ReadMetadata readMetadata) {
        this.options = options;
        this.queueDirectory = queueDirectory;
        this.queueFileMap = queueFileMap;
        this.writeSequenceNumber = writeSequenceNumber;
        this.readMetadata = readMetadata;
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

    @SuppressWarnings("java:S1168")
    public byte[] take() throws IOException {
        if (readMetadata.getReadSequenceNumber() == writeSequenceNumber) {
            log.info("no new elements");
            return null;
        }
        if (readMetadata.isEmpty()) {
            Map.Entry<Long, QueueFile> firstEntry = queueFileMap.firstEntry(); // blocking operation
            if (firstEntry != null) {
                readMetadata.setCurrentReadFile(firstEntry.getKey());
                readMetadata.setCurrentReadOffset(0);
                readMetadata.setReadSequenceNumber(firstEntry.getKey());
                currentReadFile = firstEntry.getValue();
            }
        }
        if (readMetadata.isEmpty()) {
            throw new IllegalStateException("queue empty");
        }
        Map.Entry<Long, QueueFile> floorEntry = queueFileMap.floorEntry(readMetadata.getReadSequenceNumber());
        if (floorEntry == null) {
            log.info("queue file was deleted, skipping entries");
            readMetadata.clear();
            return take(); // retry
        }
        long entryFileId = floorEntry.getKey();
        if (entryFileId != readMetadata.getCurrentReadFile()) {
            readMetadata.setCurrentReadFile(entryFileId);
            readMetadata.setCurrentReadOffset(0);
            compactionService.submit(() -> deleteQueueFile(currentReadFile.getStartSequenceNumber()));
            currentReadFile = floorEntry.getValue();
        }
        Map.Entry<Record, Integer> readRecordReturn = currentReadFile.read(readMetadata.getCurrentReadOffset());
        Record readRecord = readRecordReturn.getKey();
        int newFileOffset = readRecordReturn.getValue();
        try {
            return readRecord.getValue();
        } finally {
            readMetadata.setCurrentReadOffset(newFileOffset);
            readMetadata.setReadSequenceNumber(readRecord.getHeader().getSequenceNumber() + 1);
        }
    }

    private void deleteQueueFile(long fileStartSequenceNumber) {
        if (fileStartSequenceNumber == currentQueueFile.getStartSequenceNumber()) {
            return;
        }
        QueueFile toBeDeletedFile = queueFileMap.get(fileStartSequenceNumber);
        if (toBeDeletedFile != null) {
            queueFileMap.remove(fileStartSequenceNumber);
            toBeDeletedFile.delete();
        }
    }

    @Override
    public void close() throws IOException {
        boolean rlock = writeLock.lock();
        try {
            compactionService.shutdown();
            if (currentQueueFile != null) {
                currentQueueFile.close();
            }
            for (QueueFile queueFile : queueFileMap.values()) {
                queueFile.close();
            }
            queueDirectory.close();
            if (readMetadata != null) {
                readMetadata.close();
            }
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
            currentQueueFile.flushToDisk();
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

    public String stats() {
        List<String> dataFiles = new ArrayList<>(queueFileMap.size());
        for (QueueFile queueFile : queueFileMap.values()) {
            dataFiles.add(queueFile.getFile().getName());
        }
        return new LadderQueueStats(
                readMetadata.getReadSequenceNumber(),
                writeSequenceNumber,
                (currentReadFile == null) ? null : currentReadFile.getFile().getName(),
                (currentQueueFile == null) ? null : currentQueueFile.getFile().getName(),
                dataFiles
        ).toString();
    }
}

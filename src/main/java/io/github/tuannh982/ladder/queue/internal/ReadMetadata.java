package io.github.tuannh982.ladder.queue.internal;

import lombok.Getter;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class ReadMetadata implements Closeable {
    private static final String METADATA_FILENAME = "READ_METADATA";
    private static final int METADATA_SIZE = 8 + 4 + 8; // file_id (file start_sequence_number) (8), file_offset (4), read_sequence_number (8)
    private static final int READ_FILE_OFFSET = 0;
    private static final int READ_OFFSET_OFFSET = 8;
    private static final int READ_SEQUENCE_NUMBER = 8 + 4;

    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    @Getter
    private long currentReadFile;
    @Getter
    private int currentReadOffset;
    @Getter
    private long readSequenceNumber;

    private ReadMetadata(FileChannel channel, MappedByteBuffer buffer) {
        this.channel = channel;
        this.buffer = buffer;
        this.currentReadFile = buffer.getLong(READ_FILE_OFFSET);
        this.currentReadOffset = buffer.getInt(READ_OFFSET_OFFSET);
        this.readSequenceNumber = buffer.getLong(READ_SEQUENCE_NUMBER);
    }

    public void setCurrentReadFile(long value) {
        buffer.putLong(READ_FILE_OFFSET, value);
        currentReadFile = value;
    }

    public void setCurrentReadOffset(int value) {
        buffer.putInt(READ_OFFSET_OFFSET, value);
        currentReadOffset = value;
    }

    public void setReadSequenceNumber(long value) {
        buffer.putLong(READ_SEQUENCE_NUMBER, value);
        readSequenceNumber = value;
    }

    public void clear() {
        setCurrentReadFile(-1);
        setCurrentReadOffset(-1);
        setReadSequenceNumber(-1);
    }

    public boolean isEmpty() {
        return currentReadFile == -1 && currentReadOffset == -1 && readSequenceNumber == -1;
    }

    @SuppressWarnings("java:S2095")
    public static ReadMetadata open(QueueDirectory queueDirectory) throws IOException {
        Path path = queueDirectory.path().resolve(METADATA_FILENAME);
        File file = path.toFile();
        boolean fileExists = Files.exists(path);
        RandomAccessFile backingFile = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = backingFile.getChannel();
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, METADATA_SIZE);
        ReadMetadata readMetadata = new ReadMetadata(fileChannel, buffer);
        if (!fileExists) {
            readMetadata.clear();
        }
        return readMetadata;
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
    }
}

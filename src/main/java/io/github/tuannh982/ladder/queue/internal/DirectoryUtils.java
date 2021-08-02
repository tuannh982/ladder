package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.commons.io.FileUtils;
import io.github.tuannh982.ladder.queue.internal.file.QueueFile;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DirectoryUtils {
    public static final Pattern DATA_FILE_PATTERN = Pattern.compile("^([0-9]+)\\.data$");

    // all storage file name is number
    public static long fileId(File file, Pattern pattern) {
        Matcher matcher = pattern.matcher(file.getName());
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Cannot extract file id for file " + file.getPath());
    }

    public static File[] dataFiles(File dir) {
        try {
            File[] files = FileUtils.ls(dir, DATA_FILE_PATTERN);
            Arrays.sort(files, Comparator.comparingLong(file -> fileId(file, DATA_FILE_PATTERN)));
            return files;
        } catch (IOException ignored) { /*never happen*/ }
        return new File[0];
    }

    private static long getMaxSequenceNumberFromQueueFile(QueueFile file) throws IOException {
        Iterator<Record> iterator = file.iterator();
        Record entry = null;
        while (iterator.hasNext()) {
            entry = iterator.next();
        }
        if (entry == null) {
            return Long.MIN_VALUE;
        } else {
            return entry.getHeader().getSequenceNumber();
        }
    }

    @SuppressWarnings({"java:S3776", "java:S4042", "java:S899"})
    public static Map.Entry<NavigableMap<Long, QueueFile>, Long> buildQueueFileMap(
            QueueDirectory queueDirectory,
            ReadMetadata readMetadata,
            LadderQueueOptions options
    ) throws IOException {
        File[] dataFiles = queueDirectory.dataFiles();
        NavigableMap<Long, QueueFile> queueFileMap = new ConcurrentSkipListMap<>();
        long fileStartSequenceNumber = Long.MIN_VALUE;
        if (readMetadata.isEmpty()) {
            for (File dataFile : dataFiles) {
                fileStartSequenceNumber = fileId(dataFile, DATA_FILE_PATTERN);
                QueueFile queueFile = QueueFile.open(fileStartSequenceNumber, queueDirectory, options);
                queueFileMap.put(fileStartSequenceNumber, queueFile);
            }
        } else {
            int floorEntryIndex = -1;
            for (int i = 0; i < dataFiles.length; i++) {
                fileStartSequenceNumber = fileId(dataFiles[i], DATA_FILE_PATTERN);
                if (fileStartSequenceNumber <= readMetadata.getReadSequenceNumber()) {
                    if (floorEntryIndex > 0) {
                        boolean b = dataFiles[floorEntryIndex].delete(); // delete stale file
                        if (!b) {
                            log.error("fail to delete file " + dataFiles[floorEntryIndex].getName());
                        }
                    }
                    floorEntryIndex = i;
                }
            }
            for (int i = floorEntryIndex; i < dataFiles.length; i++) {
                QueueFile queueFile = QueueFile.open(fileStartSequenceNumber, queueDirectory, options);
                queueFileMap.put(fileStartSequenceNumber, queueFile);
            }
        }
        long maxSequenceNumber = Long.MIN_VALUE;
        if (fileStartSequenceNumber != Long.MIN_VALUE) {
            QueueFile lastQueueFile = queueFileMap.get(fileStartSequenceNumber);
            maxSequenceNumber = getMaxSequenceNumberFromQueueFile(lastQueueFile);
        }
        return new AbstractMap.SimpleImmutableEntry<>(queueFileMap, maxSequenceNumber);
    }
}

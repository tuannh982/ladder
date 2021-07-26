package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.commons.io.FileUtils;
import io.github.tuannh982.ladder.queue.internal.file.QueueFile;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static Map.Entry<NavigableMap<Long, QueueFile>, Long> buildQueueFileMap(QueueDirectory queueDirectory, LadderQueueOptions options) throws IOException {
        File[] dataFiles = queueDirectory.dataFiles();
        NavigableMap<Long, QueueFile> queueFileMap = new ConcurrentSkipListMap<>();
        long fileStartSequenceNumber = Long.MIN_VALUE;
        for (File dataFile : dataFiles) {
            fileStartSequenceNumber = fileId(dataFile, DATA_FILE_PATTERN);
            QueueFile queueFile = QueueFile.open(fileStartSequenceNumber, queueDirectory, options);
            queueFileMap.put(fileStartSequenceNumber, queueFile);
        }
        long maxSequenceNumber = Long.MIN_VALUE;
        if (fileStartSequenceNumber != Long.MIN_VALUE) {
            QueueFile lastQueueFile = queueFileMap.get(fileStartSequenceNumber);
            maxSequenceNumber = getMaxSequenceNumberFromQueueFile(lastQueueFile);
        }
        return new AbstractMap.SimpleImmutableEntry<>(queueFileMap, maxSequenceNumber);
    }
}

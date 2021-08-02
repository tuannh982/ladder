package io.github.tuannh982.ladder.queue.internal;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class LadderQueueStats {
    private final long nextReadSequenceNumber;
    private final long nextWriteSequenceNumber;
    private final String currentReadFileName;
    private final String currentWriteFileName;
    private final List<String> dataFiles;

    @SuppressWarnings("StringBufferReplaceableByString")
    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append("LADDER QUEUE STATS---------------").append("\n");
        ret.append("nextReadSequenceNumber: ").append(nextReadSequenceNumber < 0 ? "unset" : nextReadSequenceNumber).append("\n");
        ret.append("nextWriteSequenceNumber: ").append(nextWriteSequenceNumber).append("\n");
        ret.append("currentReadFileName: ").append(currentReadFileName).append("\n");
        ret.append("currentWriteFileName: ").append(currentWriteFileName).append("\n");
        ret.append("dataFiles: ").append(dataFiles).append("\n");
        ret.append("---------------------------------").append("\n");
        return ret.toString();
    }
}

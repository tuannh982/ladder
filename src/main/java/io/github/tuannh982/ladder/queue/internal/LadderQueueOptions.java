package io.github.tuannh982.ladder.queue.internal;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(builderClassName = "Builder", buildMethodName = "build")
public class LadderQueueOptions {
    private final int maxFileSize;
    private final int dataFlushThreshold;

    public static class Builder {
        void validate(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException("validation failed at: " + message);
            }
        }

        public LadderQueueOptions build() {
            validate(maxFileSize > 0, "maxFileSize > 0");
            validate(dataFlushThreshold > 0, "dataFlushThreshold > 0");
            return new LadderQueueOptions(maxFileSize, dataFlushThreshold);
        }
    }
}

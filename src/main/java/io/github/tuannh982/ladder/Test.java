package io.github.tuannh982.ladder;

import io.github.tuannh982.ladder.queue.Queue;
import io.github.tuannh982.ladder.queue.internal.LadderQueue;
import io.github.tuannh982.ladder.queue.internal.LadderQueueOptions;

import java.io.File;
import java.io.IOException;

public class Test {
    private static byte[] toBytes(final int data) {
        return new byte[] {
                (byte)((data >> 24) & 0xff),
                (byte)((data >> 16) & 0xff),
                (byte)((data >> 8) & 0xff),
                (byte)((data) & 0xff),
        };
    }

    private static int toInt(byte[] data) {
        return
                (0xff & data[0]) << 24 |
                (0xff & data[1]) << 16 |
                (0xff & data[2]) << 8  |
                (0xff & data[3])
        ;
    }

    @SuppressWarnings("all")
    public static void main(String[] args) throws IOException {
        String path = "/home/tuannh/Desktop/test";
        Queue queue = new LadderQueue(
                new File(path),
                LadderQueueOptions.builder()
                        .dataFlushThreshold(512 * 1024)
                        .maxFileSize(100 * 1024)
                        .build()
        );
        for (int i = 0; i < 50_000; i++) {
            queue.put(toBytes(i));
            byte[] readBytes = queue.take();
            int read = toInt(readBytes);
            if (read != i) {
                System.out.println("not matched at iteration " + i);
            }
        }
        System.out.println(queue.stats());
        queue.close();
    }
}

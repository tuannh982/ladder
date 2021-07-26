package io.github.tuannh982.ladder.queue.internal;

import io.github.tuannh982.ladder.commons.number.NumberUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

@Getter
public class Record {
    public static final int HEADER_SIZE = 4 + 8 + 4; // value_size(4), sequence_number(8), checksum(4)
    public static final int HEADER_SIZE_WITHOUT_CHECKSUM = 4 + 8;
    public static final int VALUE_SIZE_OFFSET = 0;
    public static final int SEQUENCE_NUMBER_OFFSET = 4;
    public static final int CHECKSUM_OFFSET = 4 + 8;

    private final Header header;
    private final byte[] value;

    public Record(byte[] value) {
        this.value = value;
        this.header = new Header(value.length, -1, 0);
    }

    public Record(byte[] value, Header header) {
        this.value = value;
        this.header = header;
    }

    private long checksum(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, HEADER_SIZE_WITHOUT_CHECKSUM);
        crc32.update(value);
        return crc32.getValue();
    }

    public ByteBuffer[] serialize() {
        header.checksum = checksum(header.serialize()); // just for checksum, overwrite checksum later
        ByteBuffer headerBuffer = header.serialize();
        return new ByteBuffer[] {headerBuffer, ByteBuffer.wrap(value)};
    }

    public static Record deserialize(ByteBuffer buffer, Header header) {
        buffer.flip();
        byte[] value = new byte[header.valueSize];
        buffer.get(value);
        return new Record(value, header);
    }

    public boolean verifyChecksum() {
        long checksum = checksum(header.serialize());
        return checksum == header.checksum;
    }

    public int serializedSize() {
        return HEADER_SIZE + value.length;
    }

    public int valueOffset(int offset) {
        return offset + HEADER_SIZE + header.valueSize;
    }

    @AllArgsConstructor
    @Getter
    @Setter
    public static class Header {
        private int valueSize;
        private long sequenceNumber;
        private long checksum;

        public ByteBuffer serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.putInt(VALUE_SIZE_OFFSET, valueSize);
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            buffer.putInt(CHECKSUM_OFFSET, NumberUtils.fromUInt32(checksum));
            return buffer;
        }

        public static Header deserialize(ByteBuffer buffer) {
            int valueSize = buffer.getInt(VALUE_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
            long checksum = NumberUtils.toUInt32(buffer.getInt(CHECKSUM_OFFSET));
            return new Header(valueSize, sequenceNumber, checksum);
        }
    }
}

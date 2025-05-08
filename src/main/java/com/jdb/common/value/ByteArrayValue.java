package com.jdb.common.value;

import com.jdb.common.DataType;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class ByteArrayValue extends Value<byte[]> {
    public ByteArrayValue(byte[] value) {
        super(DataType.BYTE_ARRAY, value);
    }

    public static ByteArrayValue deserialize(ByteBuffer buffer, int offset) {
        short length = buffer.getShort(offset);
        offset += Short.BYTES;
        byte[] bytes = new byte[length];
        buffer.position(offset).get(bytes);
        return new ByteArrayValue(bytes);
    }

    @Override
    public int getBytes() {
        return 0;
    }

    @Override
    public int serialize(ByteBuffer buffer, int offset) {
        return 0;
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return 0;
    }
}

package com.jdb.common.value;

import com.jdb.common.DataType;

import java.nio.ByteBuffer;

public class BoolValue extends Value<Boolean> {
    public BoolValue(boolean value) {
        super(DataType.BOOLEAN, value);
    }

    public static BoolValue deserialize(ByteBuffer buffer, int offset) {
        byte b = buffer.get(offset);
        return new BoolValue(b == 1);
    }

    @Override
    public int getBytes() {
        return 1;
    }

    @Override
    public int serialize(ByteBuffer buffer, int offset) {
        byte b = value ? (byte) 1 : (byte) 0;
        buffer.put(offset, b);
        return offset + 1;
    }

    @Override
    public int compareTo(Value o) {
        throw new IllegalArgumentException("Unsupported type: " + o.getClass());
    }

    public BoolValue and(BoolValue other) {
        return new BoolValue(value && other.value);
    }

    public BoolValue or(BoolValue other) {
        return new BoolValue(value || other.value);
    }

    public BoolValue not() {
        return new BoolValue(!value);
    }
}

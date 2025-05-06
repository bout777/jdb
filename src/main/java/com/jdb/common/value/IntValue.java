package com.jdb.common.value;

import com.jdb.common.DataType;

import java.nio.ByteBuffer;

// 整型值
public class IntValue extends Value<Integer> {
    public IntValue(int value) {
        super(DataType.INTEGER, value);
    }

    public static IntValue deserialize(ByteBuffer buffer, int offset) {
        return new IntValue(buffer.getInt(offset));
    }

    @Override
    public int getBytes() {
        return Integer.BYTES;
    }

    @Override
    public int serialize(ByteBuffer buffer, int offset) {

        buffer.putInt(offset, value);
        offset += Integer.BYTES;

        return offset;
    }

    //    @Override
//    public String toString() {
//        return value.toString();
//    }
    @Override
    public int compareTo(Value o) {
        if (o instanceof IntValue) {
            return Integer.compare(value, (int) o.value);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + o.getClass());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntValue intValue = (IntValue) o;
        return value.equals(intValue.value);
    }
}

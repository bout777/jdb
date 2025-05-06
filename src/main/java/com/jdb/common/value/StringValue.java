package com.jdb.common.value;

import com.jdb.common.DataType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// 字符串值（UTF-8编码）
public class StringValue extends Value<String> {
    public StringValue(String value) {
        super(DataType.STRING, value);
    }

    public static StringValue deserialize(ByteBuffer buffer, int offset) {
        short length = buffer.getShort(offset);
        offset += Short.BYTES;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = buffer.get(offset + i);
        }


        return new StringValue(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public int getBytes() {
        return value.length() * Byte.BYTES + Short.BYTES;
    }

    //        @Override
//    public byte[] serialize() {
//        byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);
//        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + strBytes.length);
//        buffer.put((byte) type.ordinal());
//        buffer.putShort((short) strBytes.length);
//        buffer.put(strBytes);
//        return buffer.array();
//    }
    @Override
    public int serialize(ByteBuffer buffer, int offset) {
//        buffer.put(offset, (byte) type.ordinal());
//        offset += Byte.BYTES;

        byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);

        buffer.putShort(offset, (short) strBytes.length);
        offset += Short.BYTES;

//        buffer.put(strBytes,offset,strBytes.length);
        for (int i = 0; i < strBytes.length; i++) {
            buffer.put(offset + i, strBytes[i]);
        }
        offset += strBytes.length * Byte.BYTES;

        return offset;
    }

    @Override
    public int compareTo(Value o) {
        if (o instanceof StringValue) {
            return value.compareTo((String) o.value);
        }
        throw new IllegalArgumentException("Unsupported type: " + o.getClass());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringValue that = (StringValue) o;
        return value.equals(that.value);
    }
}

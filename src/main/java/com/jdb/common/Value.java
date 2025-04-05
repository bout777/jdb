package com.jdb.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// 基础类型枚举（可扩展）


// Value 基类
public abstract class Value<T> implements Comparable<Value> {
    protected final DataType type;
    protected final T value;

    public Value(DataType type, T value) {
        this.type = type;
        this.value = value;
    }

    // 反序列化静态方法（工厂模式）
    public static Value<?> deserialize(ByteBuffer buffer, int offset, DataType type) {
        switch (type) {
            case INTEGER:
                return IntValue.deserialize(buffer, offset);
            case STRING:
                return StringValue.deserialize(buffer, offset);
//            case BOOLEAN:
//                return BooleanValue.deserialize(buffer);
//            case FLOAT:
//                return FloatValue.deserialize(buffer);
//            case NULL:
//                return NullValue.INSTANCE;
            default:
                throw new IllegalArgumentException();
        }
    }

    //    public static Value<?> of(Object value) {
//        if (value instanceof Integer) {
//            return new IntValue((Integer) value);
//        } else if (value instanceof String) {
//            return new StringValue((String) value);
//        } else {
//            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
//        }
//    }
    public static Value ofInt(int value) {
        return new IntValue(value);
    }

    public static Value ofString(String value) {
        return new StringValue(value);
    }

    public abstract int getBytes();

    // 序列化为字节数组（包含类型标记）
    public abstract int serialize(ByteBuffer buffer, int offset);

    // 类型安全检查
    public <E> E getValue(Class<E> expectedType) {
        if (!expectedType.isAssignableFrom(value.getClass())) {
            throw new ClassCastException("Type mismatch: expected " +
                    expectedType + ", actual " + value.getClass());
        }
        return expectedType.cast(value);
    }

    public DataType getType() {
        return type;
    }

    public String toString() {
        return value.toString();
    }

}

// --- 具体子类型实现 ---

// 整型值
class IntValue extends Value<Integer> {
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
//        buffer.put(offset, (byte) type.ordinal());
//        offset += Byte.BYTES;

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

// 字符串值（UTF-8编码）
class StringValue extends Value<String> {
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

// 布尔值
//class BooleanValue extends Value<Boolean> {
//    public BooleanValue(boolean value) {
//        super(DataType.BOOLEAN, value);
//    }
//
//    @Override
//    public byte[] serialize() {
//        return ByteBuffer.allocate(2)
//                .put((byte) type.ordinal())
//                .put((byte) (value ? 1 : 0))
//                .array();
//    }
//
//    public static BooleanValue deserialize(ByteBuffer buffer) {
//        return new BooleanValue(buffer.get() != 0);
//    }
//}

// 浮点数值
//class FloatValue extends Value<Float> {
//    public FloatValue(float value) {
//        super(DataType.FLOAT, value);
//    }
//
//    @Override
//    public byte[] serialize() {
//        ByteBuffer buffer = ByteBuffer.allocate(1 + 4);
//        buffer.put((byte) type.ordinal());
//        buffer.putFloat(value);
//        return buffer.array();
//    }
//
//    public static FloatValue deserialize(ByteBuffer buffer) {
//        return new FloatValue(buffer.getFloat());
//    }
//}

// 空值（单例模式）
//class NullValue extends Value<Void> {
//    public static final NullValue INSTANCE = new NullValue();
//
//    private NullValue() {
//        super(DataType.NULL, null);
//    }
//
//    @Override
//    public byte[] serialize() {
//        return new byte[]{(byte) DataType.NULL.ordinal()};
//    }
//}

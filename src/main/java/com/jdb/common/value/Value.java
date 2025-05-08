package com.jdb.common.value;

import com.jdb.common.DataType;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

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
            case BOOLEAN:
                return BoolValue.deserialize(buffer,offset);
//            case FLOAT:
//                return FloatValue.deserialize(buffer);
//            case NULL:
//                return NullValue.INSTANCE;
            default:
                throw new IllegalArgumentException();
        }
    }
    private static final Pattern INTEGER_PATTERN = Pattern.compile("-?\\d+");
    public static Value<?> fromString(String str) {
        if (str == null) {
            throw new IllegalArgumentException("Cannot convert null to Value");
        }
        String s = str.trim();

        // 1. NULL
        if (s.equalsIgnoreCase("NULL")) {
            // 如果你已经实现了 NullValue
            // return NullValue.INSTANCE;
            // 暂时回退成空字符串
            return new StringValue("");
        }

        // 2. 整数
        if (INTEGER_PATTERN.matcher(s).matches()) {
            try {
                int v = Integer.parseInt(s);
                return new IntValue(v);
            } catch (NumberFormatException e) {
                // fall through to string
            }
        }

        // 3. 带引号的字符串字面量
        if ((s.startsWith("'") && s.endsWith("'")) ||
                (s.startsWith("\"") && s.endsWith("\""))) {
            // 去除首尾引号，并把 SQL 中的 '' 转为 '
            String inner = s.substring(1, s.length() - 1)
                    .replace("''", "'");
            return new StringValue(inner);
        }

        // 4. 默认当做普通字符串
        return new StringValue(s);
    }

    public static Value<Integer> of(int value) {
        return new IntValue(value);
    }

    public static Value<String> of(String value) {
        return new StringValue(value);
    }

    public static Value<Boolean> of(boolean value) {return new BoolValue(value);}

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

    @Override
    public String toString() {
        return value.toString();
    }

}

// --- 具体子类型实现 ---

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

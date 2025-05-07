package com.jdb.value;

import com.jdb.common.value.IntValue;
import com.jdb.common.value.StringValue;
import com.jdb.common.value.Value;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.Test;

public class ValueTest {
    @Test
    public void testFromString() {
        var value = Value.fromString("Hello World");
        assert value instanceof StringValue;

        value = Value.fromString("123");
        assert value instanceof IntValue;

        value = Value.fromString("-123");
        assert value instanceof IntValue;

        value = Value.fromString("'Hello World'");
        assert value instanceof StringValue;
        assert value.toString().equals("Hello World");
    }
}

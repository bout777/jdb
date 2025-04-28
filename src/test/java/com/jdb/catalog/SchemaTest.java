package com.jdb.catalog;

import com.jdb.TestUtil;
import org.junit.Test;

public class SchemaTest {
    @Test
    public void testFromString() {
        Schema expected = TestUtil.recordSchema();
        Schema actual = Schema.fromString(expected.toString());
        assert expected.equals(actual);
    }
}

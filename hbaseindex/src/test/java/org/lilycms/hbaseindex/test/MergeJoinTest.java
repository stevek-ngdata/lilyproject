package org.lilycms.hbaseindex.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilycms.hbaseindex.Conjunction;
import org.lilycms.hbaseindex.Disjunction;
import org.lilycms.hbaseindex.QueryResult;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class MergeJoinTest {
    @Test
    public void testConjunction() throws Exception {
        String[] values1 = {"a", "b", "c",           "f", "g"};
        String[] values2 = {     "b", "c", "d", "e", "f"};

        QueryResult result = new Conjunction(buildQueryResult(values1), buildQueryResult(values2));

        assertEquals("b", Bytes.toString(result.next()));
        assertEquals("c", Bytes.toString(result.next()));
        assertEquals("f", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testDisjunction() throws Exception {
        String[] values1 = {"a", "b", "c",           "f", "g"};
        String[] values2 = {     "b", "c", "d", "e", "f"};

        QueryResult result = new Disjunction(buildQueryResult(values1), buildQueryResult(values2));

        assertEquals("a", Bytes.toString(result.next()));
        assertEquals("b", Bytes.toString(result.next()));
        assertEquals("c", Bytes.toString(result.next()));
        assertEquals("d", Bytes.toString(result.next()));
        assertEquals("e", Bytes.toString(result.next()));
        assertEquals("f", Bytes.toString(result.next()));
        assertEquals("g", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    private QueryResult buildQueryResult(String[] values) {
        List<byte[]> byteValues = new ArrayList<byte[]>(values.length);

        for (String value : values) {
            byteValues.add(Bytes.toBytes(value));
        }

        return new StaticQueryResult(byteValues);
    }
}

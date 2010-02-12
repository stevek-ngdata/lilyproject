package org.lilycms.hbaseindex.test;

import org.lilycms.hbaseindex.QueryResult;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class StaticQueryResult implements QueryResult {
    private Iterator<byte[]> iterator;

    public StaticQueryResult(List<byte[]> values) {
        this.iterator = values.iterator();
    }

    public byte[] next() throws IOException {
        return iterator.hasNext() ? iterator.next() : null;
    }
}

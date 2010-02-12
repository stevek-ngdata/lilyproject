package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Merge-joins two QueryResults into one, in other words: an AND
 * operation on two indices.
 *
 * <p>This only works if the individual QueryResults return their rows in
 * sorted in increasing row-key order. This means that these should be
 * QueryResults resulting from queries that only contain equals conditions,
 * and no range conditions. Also the QueryResults should not return the
 * same row key more than once.
 *
 * <p>A Conjunction itself also returns its results in increasing row-key
 * order, and can hence serve as input to other Conjunctions.
 */
public class Conjunction implements QueryResult {
    private QueryResult result1;
    private QueryResult result2;

    public Conjunction(QueryResult result1, QueryResult result2) {
        this.result1 = result1;
        this.result2 = result2;
    }

    public byte[] next() throws IOException {
        byte[] key1 = result1.next();
        byte[] key2 = result2.next();

        if (key1 == null || key2 == null)
            return null;

        int cmp = Bytes.compareTo(key1, key2);

        while (cmp != 0) {
            if (cmp < 0) {
                while (cmp < 0) {
                    key1 = result1.next();
                    if (key1 == null)
                        return null;
                    cmp = Bytes.compareTo(key1, key2);
                }
            } else if (cmp > 0) {
                while (cmp > 0) {
                    key2 = result2.next();
                    if (key2 == null)
                        return null;
                    cmp = Bytes.compareTo(key1, key2);
                }
            }
        }

        return key1;
    }

}

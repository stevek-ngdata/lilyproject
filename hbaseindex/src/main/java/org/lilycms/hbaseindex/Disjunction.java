package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * A QueryResult which is the disjunction (= OR operation) of two other QueryResults.
 *
 * <p>The supplied QueryResults should adhere to the same requirements as for
 * {@link Conjunction}s. 
 */
public class Disjunction implements QueryResult {
    private QueryResult result1;
    private QueryResult result2;
    private byte[] key1;
    private byte[] key2;
    private boolean init = false;

    public Disjunction(QueryResult result1, QueryResult result2) {
        this.result1 = result1;
        this.result2 = result2;
    }

    public byte[] next() throws IOException {
        if (!init) {
            key1 = result1.next();
            key2 = result2.next();
            init = true;
        }

        if (key1 == null && key2 == null) {
            return null;
        } else if (key1 == null) {
            byte[] result = key2;
            key2 = result2.next();
            return result;
        } else if (key2 == null) {
            byte[] result = key1;
            key1 = result1.next();
            return result;
        }

        int cmp = Bytes.compareTo(key1, key2);

        if (cmp == 0) {
            byte[] result = key1;
            key1 = result1.next();
            key2 = result2.next();
            return result;
        } else if (cmp < 0) {
            byte[] result = key1;
            key1 = result1.next();
            return result;
        } else { // cmp > 0
            byte[] result = key2;
            key2 = result2.next();
            return result;
        }
    }
}

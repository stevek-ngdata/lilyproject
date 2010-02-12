package org.lilycms.hbaseindex;

import java.io.IOException;

/**
 * Result of executing a query.
 */
public interface QueryResult {

    /**
     * Move to and return the next result.
     *
     * @return the row key of the next matching query result.
     */
    public byte[] next() throws IOException;
}

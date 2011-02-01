package org.lilyproject.hbaseindex;

public class IndexExistsException extends RuntimeException {
    private String indexName;

    public IndexExistsException(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getMessage() {
        return "Index exists: '" + indexName + "'";
    }
}

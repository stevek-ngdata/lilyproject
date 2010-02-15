package org.lilycms.hbaseindex;

public class IndexNotFoundException extends Exception {
    private String indexName;

    public IndexNotFoundException(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getMessage() {
        return "No such index: " + indexName;
    }
}

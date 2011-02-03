package org.lilyproject.indexer.engine;

public class SolrClientConfig {
    private String requestWriter;
    private String responseParser;

    public SolrClientConfig() {

    }

    public String getRequestWriter() {
        return requestWriter;
    }

    public void setRequestWriter(String requestWriter) {
        this.requestWriter = requestWriter;
    }

    public String getResponseParser() {
        return responseParser;
    }

    public void setResponseParser(String responseParser) {
        this.responseParser = responseParser;
    }
}

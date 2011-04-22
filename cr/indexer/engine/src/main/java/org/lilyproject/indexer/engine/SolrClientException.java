package org.lilyproject.indexer.engine;


/**
 * A wrapper around exceptions that occur on SOLR, with as purpose identifying
 * the SOLR instance to which the request was sent
 */
public class SolrClientException extends Exception {
    public SolrClientException(String solrInstance, Throwable cause) {
        super("Error performing operation on SOLR " + solrInstance, cause);
    }
}

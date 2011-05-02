package org.lilyproject.indexer.engine;


/**
 * A wrapper around exceptions that occur on Solr, with as purpose identifying
 * the Solr instance to which the request was sent
 */
public class SolrClientException extends Exception {
    public SolrClientException(String solrInstance, Throwable cause) {
        super("Error performing operation on Solr " + solrInstance, cause);
    }
}

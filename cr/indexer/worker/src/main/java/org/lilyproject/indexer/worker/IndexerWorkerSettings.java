package org.lilyproject.indexer.worker;

public class IndexerWorkerSettings {
    private int listenersPerIndex = 10;
    private boolean enableLocking = false;
    private int solrMaxTotalConnections = 200;
    private int solrMaxConnectionsPerHost = 50;

    public int getListenersPerIndex() {
        return listenersPerIndex;
    }

    public void setListenersPerIndex(int listenersPerIndex) {
        this.listenersPerIndex = listenersPerIndex;
    }

    public boolean getEnableLocking() {
        return enableLocking;
    }

    public void setEnableLocking(boolean enableLocking) {
        this.enableLocking = enableLocking;
    }

    public int getSolrMaxTotalConnections() {
        return solrMaxTotalConnections;
    }

    public void setSolrMaxTotalConnections(int solrMaxTotalConnections) {
        this.solrMaxTotalConnections = solrMaxTotalConnections;
    }

    public int getSolrMaxConnectionsPerHost() {
        return solrMaxConnectionsPerHost;
    }

    public void setSolrMaxConnectionsPerHost(int solrMaxConnectionsPerHost) {
        this.solrMaxConnectionsPerHost = solrMaxConnectionsPerHost;
    }
}

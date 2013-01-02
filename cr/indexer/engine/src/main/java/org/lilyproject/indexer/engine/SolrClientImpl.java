/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.engine;

import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

public class SolrClientImpl implements SolrClient {

    private final SolrServer solrServer;

    private final String description;

    private String collection;

    public SolrClientImpl(SolrServer solrServer, String description) {
        this.solrServer = solrServer;
        this.description = description;
    }

    public SolrClientImpl(SolrServer solrServer, String collection, String description) {
        this.solrServer = solrServer;
        this.description = description;
        this.collection = collection;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public UpdateResponse add(SolrInputDocument doc) throws SolrClientException {
        UpdateRequest request = new UpdateRequest();
        request.add(doc);
        if (collection != null) {
            request.setParam("collection", collection);
        }
        try {
            return request.process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrClientException {
        UpdateRequest request = new UpdateRequest();
        request.add(docs);
        if (collection != null) {
            request.setParam("collection", collection);
        }
        try {
            return request.process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse deleteById(List<String> ids) throws SolrClientException {
        UpdateRequest request = new UpdateRequest();
        request.deleteById(ids);
        if (collection != null) {
            request.setParam("collection", collection);
        }
        try {
            return request.process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse deleteById(String id) throws SolrClientException {
        UpdateRequest request = new UpdateRequest();
        request.deleteById(id);
        if (collection != null) {
            request.setParam("collection", collection);
        }
        try {
            return request.process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse deleteByQuery(String query) throws SolrClientException {

        UpdateRequest request = new UpdateRequest();
        request.deleteByQuery(query);
        if (collection != null) {
            request.setParam("collection", collection);
        }
        try {
            return request.process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrClientException {
        UpdateRequest request = new UpdateRequest();
        request.setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher);
        if (collection != null) {
            request.setParam("collection", collection);
        }
        try {
            return request.process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse commit() throws SolrClientException {
        return commit(true, true);
    }

    @Override
    public QueryResponse query(SolrParams params) throws SolrClientException {
        if (collection != null) {
            NamedList<String> nl = new NamedList<String>();
            nl.add("collection", collection);
            params = SolrParams.wrapAppended(SolrParams.toSolrParams(nl), params);
        }
        try {
            return new QueryRequest(params).process(solrServer);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }
}

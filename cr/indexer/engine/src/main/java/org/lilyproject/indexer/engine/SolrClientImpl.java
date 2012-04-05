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

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SolrClientImpl implements SolrClient {

    private SolrServer solrServer;

    private String description;

    public SolrClientImpl(SolrServer solrServer, String description) {
        this.solrServer = solrServer;
        this.description = description;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public UpdateResponse add(SolrInputDocument doc) throws SolrClientException {
        try {
            return solrServer.add(doc);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrClientException {
        try {
            return solrServer.add(docs);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse deleteById(List<String> ids) throws SolrClientException {
        try {
            return solrServer.deleteById(ids);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse deleteById(String id) throws SolrClientException {
        try {
            return solrServer.deleteById(id);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse deleteByQuery(String query) throws SolrClientException {
        try {
            return solrServer.deleteByQuery(query);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrClientException {
        try {
            return solrServer.commit(waitFlush, waitSearcher);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public UpdateResponse commit() throws SolrClientException {
        try {
            return solrServer.commit();
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }

    @Override
    public QueryResponse query(SolrParams params) throws SolrClientException {
        try {
            return solrServer.query(params);
        } catch (Exception e) {
            throw new SolrClientException(description, e);
        }
    }
}

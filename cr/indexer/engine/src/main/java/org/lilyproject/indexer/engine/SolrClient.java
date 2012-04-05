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

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

import java.util.Collection;
import java.util.List;

/**
 * This is an interface for SolrServer (which is an abstract class).
 */
public interface SolrClient {
    /**
     * Description of this Solr server, usually its URL.
     */
    String getDescription();

    //
    //
    // The following method declarations are copied from Solr's SolrServer class, but with
    // InterruptedException added to their throws clause. This is necessary for the RetryingSolrClient.
    //
    //

    UpdateResponse add(SolrInputDocument doc) throws SolrClientException, InterruptedException;

    UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrClientException,
            InterruptedException;

    UpdateResponse deleteById(String id) throws SolrClientException, InterruptedException;

    UpdateResponse deleteById(List<String> ids) throws SolrClientException, InterruptedException;

    UpdateResponse deleteByQuery(String query) throws SolrClientException, InterruptedException;

    UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrClientException,
            InterruptedException;

    UpdateResponse commit() throws SolrClientException, InterruptedException;

    QueryResponse query(SolrParams params) throws SolrClientException, InterruptedException;
}

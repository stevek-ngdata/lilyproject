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

import java.net.ConnectException;
import java.net.UnknownHostException;

import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ErrorSwallowingSolrClientTest {

    private SolrClientMetrics solrClientMetrics;
    private SolrClient baseSolrClient;
    private SolrInputDocument document;
    private SolrClient wrappingSolrClient;

    @Before
    public void setUp() {
        solrClientMetrics = mock(SolrClientMetrics.class);
        solrClientMetrics.swallowedExceptions = mock(MetricsTimeVaryingLong.class);
        baseSolrClient = mock(SolrClient.class);
        document = mock(SolrInputDocument.class);
        wrappingSolrClient = ErrorSwallowingSolrClient.wrap(baseSolrClient, solrClientMetrics);
    }

    @Test
    public void testWrappedClient() throws SolrClientException, InterruptedException {
        wrappingSolrClient.add(document);
        verify(baseSolrClient).add(document);
        verify(solrClientMetrics.swallowedExceptions, never()).inc();
    }

    @Test(expected = SolrException.class)
    public void testWrappedClient_SolrException_HTTP_404() throws SolrClientException, InterruptedException {
        when(baseSolrClient.add(document)).thenThrow(new SolrException(ErrorCode.NOT_FOUND, "Not found"));

        wrappingSolrClient.add(document);
    }

    // SolrClientExceptions should be unwrapped to get their cause
    @Test(expected = SolrClientException.class)
    public void testWrappedClient_SolrException_HTTP_404_WrappedInSolrClientException() throws SolrClientException,
            InterruptedException {
        SolrException notFoundException = new SolrException(ErrorCode.NOT_FOUND, "Not found");
        SolrClientException solrClientException = new SolrClientException("INSTANCE", notFoundException);
        when(baseSolrClient.add(document)).thenThrow(solrClientException);

        wrappingSolrClient.add(document);
    }

    @Test
    public void testWrappedClient_SolrException_NotHTTP_404() throws SolrClientException, InterruptedException {
        when(baseSolrClient.add(document)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "Bad request"));

        UpdateResponse updateResponse = wrappingSolrClient.add(document);

        verify(solrClientMetrics.swallowedExceptions, times(1)).inc();
        assertEquals(ErrorSwallowingSolrClient.ERROR_UPDATE_RESPONSE, updateResponse);
    }

    @Test(expected = SolrException.class)
    public void testWrappedClient_ErrorCausedByUnknownHostException() throws SolrClientException, InterruptedException {
        when(baseSolrClient.add(document)).thenThrow(
                new SolrException(ErrorCode.UNKNOWN, new UnknownHostException("unknown host")));

        wrappingSolrClient.add(document);
    }

    @Test(expected = SolrException.class)
    public void testWrappedClient_ErrorCausedByConnectException() throws SolrClientException, InterruptedException {
        when(baseSolrClient.add(document)).thenThrow(new SolrException(ErrorCode.UNKNOWN, new ConnectException()));

        wrappingSolrClient.add(document);
    }

    @Test(expected = SolrClientException.class)
    public void testWrappedClient_NoLiveSolrServers() throws SolrClientException, InterruptedException {
        when(baseSolrClient.add(document)).thenThrow(
                new SolrClientException("INSTANCE", new SolrServerException(
                        "No live SolrServers available to handle this request")));

        wrappingSolrClient.add(document);
    }

    @Test
    public void testWrappedClient_GenericException() throws SolrClientException, InterruptedException {
        when(baseSolrClient.add(document)).thenThrow(new UnsupportedOperationException());

        UpdateResponse updateResponse = wrappingSolrClient.add(document);

        verify(solrClientMetrics.swallowedExceptions, times(1)).inc();
        assertEquals(ErrorSwallowingSolrClient.ERROR_UPDATE_RESPONSE, updateResponse);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWrappedClient_DontSwallowErrorsOnMethodsThatDontReturnAnUpdateResponse()
            throws SolrClientException, InterruptedException {
        SolrParams solrParams = mock(SolrParams.class);
        when(baseSolrClient.query(solrParams)).thenThrow(new UnsupportedOperationException());

        wrappingSolrClient.query(solrParams);
    }

}

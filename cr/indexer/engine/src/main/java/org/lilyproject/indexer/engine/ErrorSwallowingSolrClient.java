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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;

/**
 * A wrapper around a SolrClient that swallows certain kinds of errors that occur while indexing via a
 * {@code SolrClient}, while throwing others.
 * <p>
 * Any kind of exception that is caused by the unavailability of a Solr server or (temporary) configuration issue on a
 * Solr is thrown through, with the rationale being that these kinds of errors can be corrected by a human operator,
 * after which the indexing operations should still be executed.
 * <p>
 * Any kind of exception that is due to some kind of document issue (meaning that it's not a temporary issue that can
 * be
 * resolved by a human operator) is swallowed and recorded in a metric.
 * <p>
 * The rationale behind this class is the fact that indexing is run within the SEP (Side-Effect Processor), which has
 * its own retry mechanism. Only errors related to document invalidity should not be retried, which is why these types
 * of errors are swallowed.
 * <p>
 * This class is intended to do the inverse of the previous RetryingSolrClient.
 */
public class ErrorSwallowingSolrClient {

    /**
     * Dummy UpdateResponse that is returned in case of a swallowed exception.
     */
    static final UpdateResponse ERROR_UPDATE_RESPONSE = new UpdateResponse();

    static {
        // Ensure we won't get NullPointerExceptions on the toString of ERROR_UPDATE_RESPONSE
        ERROR_UPDATE_RESPONSE.setResponse(new NamedList<Object>());
    }

    private ErrorSwallowingSolrClient() {
    }

    public static SolrClient wrap(SolrClient solrClient, SolrClientMetrics solrClientMetrics) {
        ErrorSwallowingSolrClientInvocationHandler handler = new ErrorSwallowingSolrClientInvocationHandler(solrClient,
                solrClientMetrics);
        return (SolrClient) Proxy.newProxyInstance(SolrClient.class.getClassLoader(), new Class[]{SolrClient.class},
                handler);
    }

    private static class ErrorSwallowingSolrClientInvocationHandler implements InvocationHandler {

        private SolrClient baseSolrClient;
        private SolrClientMetrics clientMetrics;
        private final Log log = LogFactory.getLog(getClass());

        ErrorSwallowingSolrClientInvocationHandler(SolrClient solrClient, SolrClientMetrics clientMetrics) {
            this.baseSolrClient = solrClient;
            this.clientMetrics = clientMetrics;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(baseSolrClient, args);
            } catch (InvocationTargetException ite) {
                Throwable throwable = ite.getTargetException();
                final Throwable originalThrowable = throwable;

                if (!method.getReturnType().equals(UpdateResponse.class)) {
                    throw originalThrowable;
                }

                if (throwable instanceof SolrClientException) {
                    throwable = throwable.getCause();
                }

                Throwable cause = throwable.getCause();

                if (throwable instanceof SolrException &&
                        ((SolrException) throwable).code() == ErrorCode.NOT_FOUND.code) {
                    throw originalThrowable;
                } else if (cause != null && cause instanceof UnknownHostException) {
                    throw originalThrowable;
                } else if (cause != null && cause instanceof ConnectException) {
                    throw originalThrowable;
                } else if (cause == null && throwable instanceof SolrServerException
                        && throwable.getMessage().equals("No live SolrServers available to handle this request")) {
                    throw originalThrowable;
                }
                log.warn("Swallowing Solr exception", originalThrowable);
                clientMetrics.swallowedExceptions.inc();
                return ERROR_UPDATE_RESPONSE;
            }
        }
    }
}

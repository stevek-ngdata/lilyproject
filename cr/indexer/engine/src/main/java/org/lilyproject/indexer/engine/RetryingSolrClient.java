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
import org.apache.solr.common.SolrException;

/**
 * Puts a wrapper around SolrClient that will pause and retry on certain kinds of errors. This avoids
 * records being skipped from indexing and logs flooding with error stack traces in case of a (temporary)
 * Solr connection problem.
 */
public class RetryingSolrClient {

    public static SolrClient wrap(SolrClient solrClient, SolrClientMetrics metrics) {
        RetryingSolrClientInvocationHandler handler = new RetryingSolrClientInvocationHandler(solrClient, metrics);
        return (SolrClient)Proxy.newProxyInstance(SolrClient.class.getClassLoader(), new Class[]{SolrClient.class},
                handler);
    }

    private static class RetryingSolrClientInvocationHandler implements InvocationHandler {
        private SolrClient solrClient;
        private SolrClientMetrics metrics;
        private Log log = LogFactory.getLog("org.lilyproject.indexer.solrconnection");

        public RetryingSolrClientInvocationHandler(SolrClient solrClient, SolrClientMetrics metrics) {
            this.solrClient = solrClient;
            this.metrics = metrics;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // In case of failures, we keep retrying indefinitely, rather than given up after a certain number
            // of attempts. This is on purpose: the goal is that the IndexUpdater's (MQ listeners) would block
            // until Solr becomes available. The only other alternative they have is to log the error and accept
            // the next message, but then an index update would have been lost and we'd be back in the same
            // situation anyway.
            int attempt = 0;
            while (true) {
                try {
                    return method.invoke(solrClient, args);
                } catch (InvocationTargetException ite) {
                    Throwable throwable = ite.getTargetException();
                    final Throwable originalThrowable = throwable;

                    if (throwable instanceof SolrClientException) {
                        throwable = throwable.getCause();
                    }

                    final Throwable cause = throwable.getCause();

                    if (throwable instanceof SolrException) {
                        // Get the HTTP status code
                        int code = ((SolrException)throwable).code();
                        if (code == 404) {
                            // The user has probably configured an incorrect path in the Solr URL
                            backOff(attempt, "'Not Found' exception connecting to Solr " + solrClient.getDescription() +
                                    ". Incorrect path in Solr URL?");
                        } else {
                            throw originalThrowable;
                        }
                    } else if (cause != null && cause instanceof UnknownHostException) {
                        backOff(attempt, "Solr host unknown " + solrClient.getDescription() + ".");
                    } else if (throwable.getCause() != null && throwable.getCause() instanceof ConnectException) {
                        backOff(attempt, "Could not connect to Solr " + solrClient.getDescription() + ".");
                    } else if (throwable.getCause() == null && throwable instanceof SolrServerException &&
                            throwable.getMessage().equals("No live SolrServers available to handle this request")) {
                        backOff(attempt, "Could not connect to Solr " + solrClient.getDescription() + ".");
                    } else {
                        throw originalThrowable;
                    }
                }
                metrics.retries.inc();
                attempt++;
            }
        }

        private void backOff(int attempt, String message) throws InterruptedException {
            int pause = getBackOff(attempt);
            log.error(message + " Will sleep " + pause + "ms and retry (attempt " + attempt + ")");
            Thread.sleep(pause);
        }

        private int getBackOff(int attempt) {
            if (attempt < 10) {
                return 500;
            } else {
                return 3000;
            }
        }
    }
}

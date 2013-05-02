/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.client.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.client.NoServersException;
import org.lilyproject.client.RetryConf;
import org.lilyproject.repository.api.ConcurrentRecordUpdateException;
import org.lilyproject.repository.api.IOBlobException;
import org.lilyproject.repository.api.IORecordException;
import org.lilyproject.repository.api.IOTypeException;
import org.lilyproject.repository.api.RetriesExhaustedException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class RetryUtil {
    private Log log = LogFactory.getLog(getClass());
    private RetryConf retryConf;

    protected RetryUtil(RetryConf retryConf) {
        this.retryConf = retryConf;
    }

    public static <T> T getRetryingInstance(T delegate, Class<T> delegateType, RetryConf retryConf) {
        RetryUtil retryUtil = new RetryUtil(retryConf);
        InvocationHandler ih = new RetryingInvocationHandler<T>(delegate, retryUtil);
        T retryingInstance = (T)Proxy.newProxyInstance(RetryUtil.class.getClassLoader(),
                new Class[]{delegateType}, ih);
        return retryingInstance;
    }

    private static final class RetryingInvocationHandler<T> implements InvocationHandler {
        private final T delegate;
        private final RetryUtil retryUtil;

        private RetryingInvocationHandler(T delegate, RetryUtil retryUtil) {
            this.delegate = delegate;
            this.retryUtil = retryUtil;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                return null;
            }

            return retryUtil.retry(delegate, method, args);
        }
    }

    public Object retry(Object delegate, Method method, Object[] args) throws Throwable {
        long startedAt = System.currentTimeMillis();
        int attempt = 0;

        while (true) {
            try {
                return method.invoke(delegate, args);
            } catch (Throwable throwable) {
                handleThrowable(throwable, method, startedAt, attempt);
            }

            attempt++;
        }
    }

    public void handleThrowable(Throwable throwable, Method method, long startedAt, int attempt) throws Throwable {

        if (throwable instanceof InvocationTargetException) {
            throwable = ((InvocationTargetException)throwable).getTargetException();
        }

        if (throwable instanceof InterruptedException) {
            throw throwable;
        }

        if (throwable instanceof IORecordException || throwable instanceof IOBlobException ||
                throwable instanceof IOTypeException || throwable instanceof ConcurrentRecordUpdateException ||
                throwable instanceof NoServersException) {

            boolean callInitiated = true;
            if (throwable.getCause() instanceof NoServersException) {
                // I initially thought we could also assume the request was not yet launched in case of
                // ConnectException with msg "Connection refused". However, at least with the Avro HttpTransceiver,
                // this exception can also occur when the connection is lost between writing the request
                // and reading the response. On reading the response, the Java URLConnection will see
                // there is no connection anymore and reestablish it, hence giving a "connection refused" error.
                // In this situation, the request is sent out by the server, so it is not safe to simply redo it.
                callInitiated = false;
            }
            handleRetry(method, startedAt, attempt, callInitiated, throwable);
        } else {
            throw throwable;
        }
    }

    public void handleRetry(Method method, long startedAt, int attempt,
            boolean callInitiated, Throwable throwable) throws Throwable {

        long timeSpentRetrying = System.currentTimeMillis() - startedAt;
        if (timeSpentRetrying > retryConf.getRetryMaxTime()) {
            throw new RetriesExhaustedException(getOpString(method), attempt, timeSpentRetrying, throwable);
        }

        String methodName = method.getName();

        boolean retry = false;

        // Since the "newSomething" methods are simple factory methods, put them in the same class as reads
        // TODO: the methods starting with get include the blob methods getInputStream and getOutputStream,
        //       which should probably have a different treatment
        if ((methodName.startsWith("read") || methodName.startsWith("get") || methodName.startsWith("new"))
                && retryConf.getRetryReads()) {
            retry = true;
        } else if (methodName.equals("createOrUpdate") && retryConf.getRetryCreateOrUpdate()) {
            retry = true;
        } else if (methodName.startsWith("update") && retryConf.getRetryUpdates()) {
            retry = true;
        } else if (methodName.startsWith("delete") && retryConf.getRetryDeletes()) {
            retry = true;
        } else if (methodName.startsWith("create") && retryConf.getRetryCreate() &&
                (!callInitiated || retryConf.getRetryCreateRiskDoubles())) {
            retry = true;
        }

        if (retry) {
            int sleepTime = getSleepTime(attempt);
            if (log.isDebugEnabled() || log.isInfoEnabled()) {
                String message = "Sleeping " + sleepTime + "ms before retrying operation " +
                        getOpString(method) + " attempt " + attempt +
                        " failed due to " + throwable.toString();
                if (log.isDebugEnabled()) {
                    log.debug(message, throwable);
                } else if (log.isInfoEnabled()) {
                    log.info(message);
                }
            }
            Thread.sleep(sleepTime);
        } else {
            throw throwable;
        }
    }

    private int getSleepTime(int attempt) throws InterruptedException {
        int pos =
                attempt < retryConf.getRetryIntervals().length ? attempt : retryConf.getRetryIntervals().length - 1;
        return retryConf.getRetryIntervals()[pos];
    }

    private String getOpString(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName();
    }
}

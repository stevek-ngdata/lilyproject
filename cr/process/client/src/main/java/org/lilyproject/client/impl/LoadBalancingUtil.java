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

import org.lilyproject.repository.api.RepositoryException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class LoadBalancingUtil {
    /**
     * Wraps an object so that each method call on it will be dispatched among one of the available instances
     * provided by the {@link LBInstanceProvider}.
     */
    public static <T> T getLoadBalancedInstance(LBInstanceProvider<T> provider, Class<T> delegateType,
            String repositoryName, String tableName) {
        InvocationHandler ih = new LoadBalancingInvocationHandler<T>(provider, repositoryName, tableName);
        return (T)Proxy.newProxyInstance(LoadBalancingUtil.class.getClassLoader(), new Class[]{delegateType}, ih);
    }

    private static final class LoadBalancingInvocationHandler<T> implements InvocationHandler {
        private final LBInstanceProvider<T> provider;
        private final String repositoryName;
        private final String tableName;

        private LoadBalancingInvocationHandler(LBInstanceProvider<T> provider, String repositoryName, String tableName) {
            this.provider = provider;
            this.repositoryName = repositoryName;
            this.tableName = tableName;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                return null;
            }

            try {
                T delegate = provider.getInstance(repositoryName, tableName);
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    public static interface LBInstanceProvider<T> {
        /**
         * This method should return an object whose operations (method calls) are performed against one of the
         * available lily servers, e.g. a repository object representing a connection to one specific lily server.
         * These objects should be thread safe and its operations should be stateless (in the REST sense,
         * i.e. it should not matter that in a sequence of method calls, each one is executed against a different
         * underlying object).
         */
        T getInstance(String repositoryName, String tableName) throws RepositoryException, InterruptedException;
    }
}

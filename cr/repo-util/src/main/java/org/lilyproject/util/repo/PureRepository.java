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
package org.lilyproject.util.repo;

import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Repository;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class PureRepository {
    /**
     * Wraps a repository instance in order to disallow calling any methods that are table-scoped, i.e. that are
     * defined on the LTable interface.
     *
     * <p>The relevance of this is when you want to pass around a Repository instance but want to avoid by
     * accident calling table-scoped operations on it, because the Repository might not represent the desired
     * table.</p>
     */
    public static Repository wrap(Repository repository) {
        return (Repository)Proxy.newProxyInstance(Repository.class.getClassLoader(), new Class[]{Repository.class},
                new PureRepositoryIH(repository));
    }

    public static class PureRepositoryIH implements InvocationHandler {
        private Repository delegate;

        public PureRepositoryIH(Repository repository) {
            this.delegate = repository;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == LTable.class) {
                throw new IllegalStateException("This repository instance does not allow calling "
                        + "LTable-scoped operations.");
            } else {
                try {
                    return method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
            }
        }
    }
}

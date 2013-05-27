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
package org.lilyproject.server.modules.repository;

import org.lilyproject.repository.api.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class RepositoryDecoratorChain {
    public static final String UNDECORATED_REPOSITORY_KEY = "org.lilyproject.undecoratedrepository";

    private List<Entry> entries = new ArrayList<Entry>();

    protected void addEntryAtStart(String decoratorName, Repository repository) {
        entries.add(0, new Entry(decoratorName, repository));
    }

    /**
     * Returns the repository with all the decorators applied.
     */
    public Repository getFullyDecoratoredRepository() {
        return entries.get(0).repository;
    }

    public Repository getDecorator(String name) {
        for (Entry entry : entries) {
            if (entry.name.equals(name)) {
                return entry.repository;
            }
        }
        throw new NoSuchElementException("No repository decorator named " + name);
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public static class Entry {
        String name;
        Repository repository;

        public Entry(String name, Repository repository) {
            this.name = name;
            this.repository = repository;
        }
    }
}

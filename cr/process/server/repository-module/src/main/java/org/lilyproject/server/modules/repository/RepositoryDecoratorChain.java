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

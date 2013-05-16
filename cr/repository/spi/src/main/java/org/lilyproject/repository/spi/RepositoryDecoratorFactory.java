package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.Repository;

public interface RepositoryDecoratorFactory {
    /**
     * This will be called once for each repository-table couple, the first time that that repository
     * is requested.
     *
     * <p>In most cases, you will want to extend the actual decorator on {@link BaseRepositoryDecorator},
     * passing the supplied delegate repository to its constructor.</p>
     */
    Repository createInstance(Repository delegate);
}

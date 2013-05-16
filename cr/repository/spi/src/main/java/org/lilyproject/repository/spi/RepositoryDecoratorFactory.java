package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.Repository;

public interface RepositoryDecoratorFactory {
    Repository createInstance(Repository delegate);
}

package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.Repository;

public interface RepositoryDecorator extends Repository {
    void setDelegate(Repository repository);
}

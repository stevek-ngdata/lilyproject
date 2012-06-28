package org.lilyproject.server.modules.repository;

import org.lilyproject.repository.api.Repository;
import org.lilyproject.util.repo.PrematureRepository;

public class PrematureRepositoryActivator {
    public PrematureRepositoryActivator(PrematureRepository prematureRepository, Repository repository) {
        prematureRepository.setRepository(repository);
    }
}

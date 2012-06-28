package org.lilyproject.util.repo;

import org.lilyproject.repository.api.Repository;

/**
 * A repository implementation on which all calls will block indefinitely until
 * the {@link #setRepository(Repository)} method has been called to set a repository
 * to which the calls will be delegated.
 *
 * This provides a solution for circular dependencies in the Lily-server startup scenario.
 * E.g. some services need to be started before the repository because they must be active
 * from the moment the first repository operation is executed, but they depend themselves
 * also on the Repository.
 *
 * <p>In some cases, the Repository can provide a reference to itself when calling these
 * services, but in some cases this is not possible, and there this Repository object
 * helps. It will <b>block</b> calls until the Repository is available. The Repository
 * is made available by calling the setRepository method.
 */
public interface PrematureRepository extends Repository {
    void setRepository(Repository repository);
}

package org.lilyproject.tools.import_.json;

import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Repository;
/**
 *
 * Transforms string representations of links that might not be valid in to valid Links
 *
 */
public interface LinkTransformer {
    Link transform(String linkTextValue, Repository repository);
}

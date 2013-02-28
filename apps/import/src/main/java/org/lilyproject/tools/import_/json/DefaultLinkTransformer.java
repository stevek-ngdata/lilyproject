package org.lilyproject.tools.import_.json;

import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.RepositoryManager;

public class DefaultLinkTransformer implements LinkTransformer {

    @Override
    public Link transform(String linkTextValue, RepositoryManager repositoryManager) {
        return Link.fromString(linkTextValue, repositoryManager.getIdGenerator());
    }

}

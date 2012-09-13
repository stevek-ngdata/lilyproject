package org.lilyproject.tools.import_.json;

import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Repository;

public class DefaultLinkTransformer implements LinkTransformer {

    @Override
    public Link transform(String linkTextValue, Repository repository) {
        return Link.fromString(linkTextValue, repository.getIdGenerator());
    }

}

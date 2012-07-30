package org.lilyproject.indexer.model.indexerconf;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;
import org.lilyproject.repository.api.QName;

public class FieldNameTemplateValidator extends AbstractNameTemplateValidator {

    private static final Set<Class> SUPPORTED_TYPES = Sets.<Class>newHashSet(
            FieldTemplatePart.class,
            VariantPropertyTemplatePart.class,
            LiteralTemplatePart.class);

    public FieldNameTemplateValidator(Set<String> variantProperties, Set<QName> fields) {
        super(SUPPORTED_TYPES, Collections.<String>emptySet(), Collections.<String>emptySet(), variantProperties,
                fields);
    }
}

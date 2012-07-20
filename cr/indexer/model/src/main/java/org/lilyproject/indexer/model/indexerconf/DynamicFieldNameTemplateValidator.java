package org.lilyproject.indexer.model.indexerconf;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;
import org.lilyproject.repository.api.QName;

public class DynamicFieldNameTemplateValidator extends AbstractNameTemplateValidator {

    private static final Set<Class> SUPPORTED_TYPES = Sets.<Class>newHashSet(
            LiteralTemplatePart.class,
            VariableTemplatePart.class,
            ConditionalTemplatePart.class);

    private static final Set<String> SUPPORTED_BOOLEAN_VARIABLES = Sets.newHashSet("list", "multiValue");

    public DynamicFieldNameTemplateValidator(Set<String> variables) {
        super(SUPPORTED_TYPES, variables, SUPPORTED_BOOLEAN_VARIABLES, Collections.<String>emptySet(),
                Collections.<QName>emptySet());
    }

    public DynamicFieldNameTemplateValidator(Set<String> variables, Set<String> booleanVariables) {
        super(SUPPORTED_TYPES, variables, booleanVariables, Collections.<String>emptySet(),
                Collections.<QName>emptySet());
    }
}

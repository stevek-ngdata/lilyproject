package org.lilyproject.tools.restresourcegenerator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation to put on JAX-RS classes for which table and tenant+table based variants should be generated.
 */
@Target({ElementType.TYPE})
public @interface GenerateTenantAndTableResource {
}

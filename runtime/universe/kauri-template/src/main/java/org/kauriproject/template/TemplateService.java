/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kauriproject.template;

import java.io.FileNotFoundException;

import org.kauriproject.template.el.FunctionRegistry;
import org.kauriproject.template.source.SourceResolver;
import org.kauriproject.template.security.AccessDecider;
import org.xml.sax.SAXException;

/**
 * Interface for a high level convenience class, that gives shielded access to the templating engine and can
 * add some additional features such as caching.
 */
public interface TemplateService {

    /**
     * Create execution context with default values.
     */
    ExecutionContext createContext();

    /**
     * Create a template execution context that can be used to generate a template.
     * 
     * @see #generateTemplate(String, ExecutionContext, TemplateResult)
     */
    ExecutionContext createContext(SourceResolver sourceResolver, TemplateContext templateContext,
            FunctionRegistry functionRegistry, AccessDecider accessDecider);

    /**
     * The template at the given location will be resolved and executed against a default execution context.
     */
    void generateTemplate(String templateLocation) throws FileNotFoundException, SAXException;

    void generateTemplate(String templateLocation, ExecutionContext executionContext)
            throws FileNotFoundException, SAXException;

    /**
     * The template at the given location will be resolved and executed against the given execution context.
     */
    void generateTemplate(String templateLocation, ExecutionContext executionContext, TemplateResult result)
            throws FileNotFoundException, SAXException;

    /**
     * Build the template at the given location - or simply retrieve it from cache if it is still valid.
     */
    CompiledTemplate buildTemplate(String templateLocation, ExecutionContext executionContext)
            throws FileNotFoundException, SAXException;

    /**
     * Build the template at the given location - or simply retrieve it from cache if it is still valid.
     * 
     * @param silent
     *            This parameter (if true) can restrict the compilation to kauri-elements, output statements
     *            will thus be silenced.
     */
    CompiledTemplate buildTemplate(String templateLocation, ExecutionContext executionContext, boolean silent)
            throws FileNotFoundException, SAXException;

}

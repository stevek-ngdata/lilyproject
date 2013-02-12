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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.KauriSaxHandler.OutputFormat;
import org.kauriproject.template.el.FunctionRegistry;
import org.kauriproject.template.source.ClasspathSourceResolver;
import org.kauriproject.template.source.Source;
import org.kauriproject.template.source.SourceResolver;
import org.kauriproject.template.source.Validity;
import org.kauriproject.template.security.AccessDecider;
import org.xml.sax.SAXException;

/**
 * Default implementation of a {@link TemplateService}. This implementation adds some basic caching to the
 * templating module.
 */
public class DefaultTemplateService implements TemplateService {

    private static Log log = LogFactory.getLog(DefaultTemplateService.class);

    /**
     * Cache of compiled templates.
     */
    private Map<String, CompiledTemplate> cache;

    /**
     * Constructs a default template service.
     */
    public DefaultTemplateService() {
        this.cache = new ConcurrentHashMap<String, CompiledTemplate>();
    }

    public ExecutionContext createContext() {
        ExecutionContext ec = new ExecutionContext();
        SourceResolver sourceResolver = new ClasspathSourceResolver();
        ec.setSourceResolver(sourceResolver);
        ec.setTemplateContext(new DefaultTemplateContext());
        return ec;
    }

    public ExecutionContext createContext(SourceResolver sourceResolver, TemplateContext templateContext,
            FunctionRegistry functionRegistry, AccessDecider accessDecider) {
        return new ExecutionContext(sourceResolver, templateContext, functionRegistry, accessDecider);
    }

    public void generateTemplate(String templateLocation) throws FileNotFoundException, SAXException {
        generateTemplate(templateLocation, null, null);
    }

    public void generateTemplate(String templateLocation, ExecutionContext executionContext)
            throws FileNotFoundException, SAXException {
        generateTemplate(templateLocation, executionContext, null);
    }

    public void generateTemplate(String templateLocation, ExecutionContext executionContext,
            TemplateResult result) throws FileNotFoundException, SAXException {
        if (result == null) {
            KauriSaxHandler ksh = new KauriSaxHandler(System.out, OutputFormat.XML, "UTF-8", true, false);
            result = new TemplateResultImpl(ksh);
        }
        CompiledTemplate compiledTemplate = buildTemplate(templateLocation, executionContext);
        DefaultTemplateExecutor executor = new DefaultTemplateExecutor();
        executor.execute(compiledTemplate, null, executionContext, result);
        result.flush();
    }

    public CompiledTemplate buildTemplate(String templateLocation, ExecutionContext executionContext) {
        return buildTemplate(templateLocation, executionContext, false);
    }

    public CompiledTemplate buildTemplate(String templateLocation, ExecutionContext executionContext,
            boolean silent) {
        if (executionContext == null) {
            executionContext = createContext();
        }

        String templateKey = templateLocation;
        if (silent) {
            templateKey += "-silent";
        }

        Object sourceContext = SourceResolver.ACTIVE_SOURCE_CONTEXT.get();
        if (sourceContext != null) {
            // The toString() method of the sourceContext is expected to return an appropriate
            // value to include in the cache key.
            templateKey += "-" + sourceContext;
        }

        CompiledTemplate compiledTemplate = cache.get(templateKey);

        Source templateSource = null;
        try {
            boolean build = false;
            if (compiledTemplate == null) {
                build = true;
                log.debug("build needed");
            } else {
                Validity.Mode validityMode = compiledTemplate.getValidity().isValid();
                switch (validityMode) {
                    case VALID:
                        break;
                    case INVALID:
                        log.debug("compiledTemplate not valid, do rebuild");
                        build = true;
                        break;
                    case UNKNOWN:
                        templateSource = executionContext.getSourceResolver().resolve(templateLocation);
                        boolean valid = compiledTemplate.getValidity().isValid(templateSource.getValidity());
                        if (!valid) {
                            log.debug("compiledTemplate not valid, do rebuild");
                            build = true;
                        }
                }
            }

            if (build) {
                if (templateSource == null) {
                    templateSource = executionContext.getSourceResolver().resolve(templateLocation);
                }
                Validity newValidity = templateSource.getValidity();
                TemplateBuilder builder = new DefaultTemplateBuilder(executionContext.getFunctionRegistry(),
                        this, silent);
                compiledTemplate = builder.buildTemplate(templateSource);
                compiledTemplate.setValidity(newValidity);
                cache.put(templateKey, compiledTemplate);
            } else {
                log.debug("compiledTemplate loaded from cache");
            }
        } finally {
            if (templateSource != null) {
                templateSource.release();
            }
        }

        return compiledTemplate;
    }

    public static void main(String[] args) throws Exception {
        // java -cp
        // ".;../test-classes;$M2_REPO/log4j/log4j/1.2.12/log4j-1.2.12.jar;$M2_REPO/commons-lang/commons-lang/2.3/commons-lang-2.3.jar;$M2_REPO/commons-logging/commons-logging/1.1/commons-logging-1.1.jar;$M2_REPO/de/odysseus/juel/juel/2.1.0/juel-2.1.0.jar"
        // org.kauriproject.template.DefaultTemplateService sample.xml
        if (args.length < 1) {
            System.out
                    .println("Usage: java DefaultTemplateService <templateLocation> [var=value [var2=value2 [...]]]");
            System.out.println("e.g. java DefaultTemplateService file.xml");
        } else {
            TemplateContext templateContext = new DefaultTemplateContext();
            if (args.length > 1) {
                // load vars
                String var;
                String value;
                String arg;
                int index;
                for (int i = 1; i < args.length; i++) {
                    arg = args[i];
                    index = arg.indexOf('=');
                    if (index > 0) {
                        var = arg.substring(0, index);
                        value = arg.substring(++index, arg.length());
                        templateContext.put(var, value);
                    }
                }
            }
            TemplateService service = new DefaultTemplateService();
            ExecutionContext executionContext = service.createContext();
            executionContext.setTemplateContext(templateContext);
            service.generateTemplate(args[0], executionContext);
        }
    }
}

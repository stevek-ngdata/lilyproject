/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.runtime.rapi;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.lilyproject.runtime.conf.Conf;
import org.apache.commons.jxpath.JXPathContext;

import java.util.Properties;

/**
 * Resolves ${...} expressions in the Spring bean configuration using
 * the {@link ConfRegistry}. The syntax for the expressions is
 * ${conf-path:jxpath-expr}.
 */
public class ConfPlaceholderConfigurer extends PropertyPlaceholderConfigurer {
    private ConfRegistry confRegistry;

    public ConfPlaceholderConfigurer(ConfRegistry confRegistry) {
        this.confRegistry = confRegistry;
    }

    @Override
    protected String resolvePlaceholder(String placeholder, Properties props, int systemPropertiesMode) {
        int colonPos = placeholder.indexOf(':');
        if (colonPos == -1)
            return null;

        try {
            String confPath = placeholder.substring(0, colonPos);
            String confExpr = placeholder.substring(colonPos + 1);

            Conf conf = confRegistry.getConfiguration(confPath);
            JXPathContext context = JXPathContext.newContext(conf);
            Object value = context.getValue(confExpr);

            return value == null ? "" : value.toString();
        } catch (Exception e) {
            throw new RuntimeException("Error fetching configuration value for placeholder \"" + placeholder + "\".", e);
        }
    }
 
}

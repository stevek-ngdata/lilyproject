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
package org.kauriproject.template.el;

import java.util.Set;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import org.kauriproject.template.TemplateContext;

/**
 * Implementation of a Groovy expression.
 */
public class GroovyExpression implements Expression {

    private Script groovyScript;

    public GroovyExpression(String groovyString) {
        groovyScript = new GroovyShell().parse(groovyString);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.kauriproject.template.el.Expression#evaluate(org.kauriproject.template.TemplateContext)
     */
    public Object evaluate(TemplateContext templateContext) {
        Binding binding = new Binding();
        Set<String> keys = templateContext.getAll();
        for (String key : keys) {
            binding.setVariable(key, templateContext.get(key));
        }
        groovyScript.setBinding(binding);
        return groovyScript.run();
    }

}

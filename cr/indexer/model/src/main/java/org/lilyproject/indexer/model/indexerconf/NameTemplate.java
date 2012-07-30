/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.indexer.model.indexerconf;

import java.util.ArrayList;
import java.util.List;

public class NameTemplate {
    private String template;
    private List<TemplatePart> parts = new ArrayList<TemplatePart>();

    public NameTemplate(String template, List<TemplatePart> parts)
            throws NameTemplateException {
        // Template is stored just to be able to display it, it is already parsed
        this.template = template;
        this.parts = parts;
    }

    /**
     * format without a context value (for testing!)
     */
    public String format(NameTemplateResolver resolver) throws NameTemplateEvaluationException {
        StringBuilder result = new StringBuilder();
        for (TemplatePart part : parts) {
            result.append(resolver.resolve(part));
        }
        return result.toString();
    }

    public String getTemplate() {
        return template;
    }

    public List<TemplatePart> getParts() {
        return parts;
    }

    public void setParts(List<TemplatePart> parts) {
        this.parts = parts;
    }

}

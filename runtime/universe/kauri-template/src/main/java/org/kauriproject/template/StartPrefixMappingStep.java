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

import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class StartPrefixMappingStep extends Step {
    private String prefix;
    private String namespace;

    public StartPrefixMappingStep(Locator locator, String prefix, String namespace) {
        super(locator);
        this.prefix = prefix;
        this.namespace = namespace;
    }

    public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
        result.startPrefixMapping(prefix, namespace);
        return super.executeAndProceed(context, result);
    }
}

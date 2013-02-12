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
import org.kauriproject.xml.sax.PrefixMappingFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.ArrayList;

public class PrefixMappingFilterBlock extends TemplateBlock {
    private boolean dropAllMode;
    List<PrefixMappingFilter.PrefixAndUri> prefixAndUris = new ArrayList<PrefixMappingFilter.PrefixAndUri>();

    private Log log = LogFactory.getLog(getClass());

    public PrefixMappingFilterBlock() {
        super();
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    public void addPrefixMappings(String text) {
        text = text.trim();
        if (text.equals("#all")) {
            dropAllMode = true;
        } else {
            String[] tokens = text.split(" ");
            int tokenCount = tokens.length;
            if (tokens.length % 2 == 1) {
                tokenCount--;
                log.error("No namespace specified for prefix " + tokens[tokens.length - 1]);
            }
            for (int i = 0; i < tokenCount / 2; i+=2) {
                String prefix = tokens[i];
                String uri = tokens[i + 1];
                if (prefix.equals("#default"))
                    prefix = "";
                prefixAndUris.add(new PrefixMappingFilter.PrefixAndUri(prefix, uri));
            }
        }
    }

    class StartStep extends Step {
        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            result.addFilter(new PrefixMappingFilter(prefixAndUris, dropAllMode));
            return super.executeAndProceed(context, result);
        }
    }

    class EndStep extends Step {
        public EndStep(Locator locator) {
            super(locator);
        }

        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            result.popFilter();
            return super.executeAndProceed(context, result);
        }
    }

}

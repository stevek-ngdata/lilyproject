/*
 * Copyright 2009 Outerthought bvba and Schaubroeck nv
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

public class I18nBundleSwitchBlock extends TemplateBlock {
    private String i18nBundleName;

    public I18nBundleSwitchBlock(String i18nBundleName) {
        this.i18nBundleName = i18nBundleName;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {
        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            String oldI18nBundleName = context.getI18nBundle();
            try {
                context.setI18nBundle(i18nBundleName);
                context.getExecutor().execute(getCompiledNext(), getEndStep(), context, result);
            } finally {
                context.setI18nBundle(oldI18nBundleName);
            }
            return getEndStep().getCompiledNext();
        }
    }

    class EndStep extends Step {
        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            return getCompiledNext();
        }
    }
}

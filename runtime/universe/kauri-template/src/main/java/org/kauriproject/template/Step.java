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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.LocatorImpl;

/**
 * A link in the sequence of executable steps which denote a compiled template.
 */
public class Step {

    private static Log log = LogFactory.getLog(Step.class);

    private Step compiledNext;

    private Locator locator;

    public Step(Locator locator) {
        if (locator == null) {
            this.locator = new LocatorImpl();
        } else {
            this.locator = new LocatorImpl(locator);
        }
    }

    public Step(Step compiledNext, Locator locator) {
        this(locator);
        this.compiledNext = compiledNext;
    }

    public Step getCompiledNext() {
        return compiledNext;
    }

    public void setCompiledNext(Step compiledNext) {
        this.compiledNext = compiledNext;
    }

    /**
     * Execute this step and proceed to the next one.
     */
    public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
        // default: do nothing and proceed to compiled next
        return getCompiledNext();
    }

    public Locator getLocator() {
        return locator;
    }

    /**
     * @return (an approximation) of the document location where the associated event ends.
     */
    public String getLocation() {
        StringBuffer location = new StringBuffer();

        int colnr = locator.getColumnNumber();
        int linenr = locator.getLineNumber();
        String publicId = locator.getPublicId();
        String systemId = locator.getSystemId();

        if (publicId != null && !publicId.equals(""))
            location.append(publicId);
        else if (systemId != null && !systemId.equals(""))
            location.append(systemId);

        location.append(":").append(linenr);
        location.append(":").append(colnr);

        return location.toString();
    }

    @Override
    public String toString() {
        return super.toString() + "; location = " + getLocation();
    }
}

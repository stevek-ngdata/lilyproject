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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.KauriSaxHandler.OutputFormat;
import org.kauriproject.template.el.ELFacade;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Implementation of a "comment" template instruction.
 */
public class CommentBlock extends TemplateBlock {

    protected static Log log = LogFactory.getLog(CommentBlock.class);

    // EL
    protected ELFacade elFacade;

    public CommentBlock(ELFacade elFacade, SaxElement saxElement) {
        super(saxElement);
        this.elFacade = elFacade;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        // compiledNext: first following instruction
        // runtimeNext is endStep.compiledNext

        public StartStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) throws SAXException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            KauriSaxHandler ksh = new KauriSaxHandler(bos, OutputFormat.XML, "UTF-8", true, true);
            TemplateResult buffer = new TemplateResultImpl(ksh);
            buffer.startDocument();
            Step next = this.getCompiledNext();
            while (next != endStep) {
                next = next.executeAndProceed(context, buffer);
            }
            buffer.endDocument();
            buffer.flush();
            String value = "";
            try {
                bos.flush();
                value = bos.toString("UTF-8");
            } catch (IOException ex) {
                throw new TemplateException(ex);
            }
            result.comment(value.toCharArray(), 0, value.length());

            return endStep.getCompiledNext();
        }
    }

    class EndStep extends Step {

        // compiledNext: first following instruction after block
        // runtimeNext idem

        public EndStep(Locator locator) {
            super(locator);
        }

    }

}

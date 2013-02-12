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

import org.xml.sax.SAXException;

/**
 * The template executor is responsible for the execution of a template.
 */
public class DefaultTemplateExecutor implements TemplateExecutor {

    public void execute(Step startStep, Step endStep, ExecutionContext context, TemplateResult result) throws SAXException {
        ExecutionContext oldCurrentContext = ExecutionContext.CURRENT.get();
        try {
            ExecutionContext.CURRENT.set(context);

            if (context.getExecutor() == null) {
                context.setExecutor(this);
            }

            Step currentStep = startStep;
            while (currentStep != endStep) {
                currentStep = currentStep.executeAndProceed(context, result);
            }
        } finally {
            ExecutionContext.CURRENT.set(oldCurrentContext);
        }
    }
}

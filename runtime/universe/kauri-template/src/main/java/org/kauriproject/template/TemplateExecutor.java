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

import org.xml.sax.SAXException;

public interface TemplateExecutor {
    /**
     * Executes steps, starting from the startStep up to, but excluding, the endStep.
     */
    void execute(Step startStep, Step endStep, ExecutionContext context, TemplateResult result) throws SAXException;
}

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
import java.util.Date;

/**
 * Test the template service and it's cache.
 */
public class TemplateServiceTest extends TemplateTestBase {

    private TemplateService service;

    public TemplateServiceTest() {
        // setup service
        service = new DefaultTemplateService();
    }

    private void testService(String template, boolean check, double treshold) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // monitor duration
        long start = new Date().getTime();
        ExecutionContext tec = service.createContext();
        TemplateResult result = new TemplateResultImpl(new KauriSaxHandler.NamespaceAsAttributes(new TestHandler(bos)));
        service.generateTemplate(template, tec, result);
        long end = new Date().getTime();

        // check duration
        double duration = getDuration(start, end);
        if (TEST_DURATION) {
            log.info("duration generation: " + duration + " sec.\n");
            assertTrue("Generation was too slow: " + duration + " sec.", duration < treshold);
        }

        // check result
        if (check) {
            checkResult(template, bos, true);
        }
    }

    // ------------------
    // Tests
    // ------------------

    public void testMix() throws Exception {
        String template = "/org/kauriproject/template/mix.xml";
        if (TEST_DURATION) {
            initTresholds(template, true);
        }
        testService(template, true, flowTreshold);
    }

    public void testBig() throws Exception {
        String template = "/org/kauriproject/template/big.xml";
        if (TEST_DURATION) {
            initTresholds(template, true);
        }
        // test it three times in a row
        testService(template, true, flowTreshold);
        // it should come from cache and load fast
        testService(template, false, executionTreshold);
        testService(template, false, executionTreshold);
    }

}

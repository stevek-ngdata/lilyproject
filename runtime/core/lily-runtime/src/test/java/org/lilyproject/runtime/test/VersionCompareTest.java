/*
 * Copyright 2013 NGDATA nv
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.test;

import junit.framework.TestCase;
import org.lilyproject.runtime.ClassLoaderConfigurer;

public class VersionCompareTest extends TestCase {
    public void testCmp() {
        assertTrue(ClassLoaderConfigurer.compareVersions("2", "1") > 0);
        assertTrue(ClassLoaderConfigurer.compareVersions("1", "2") < 0);
        assertTrue(ClassLoaderConfigurer.compareVersions("1", "1") == 0);
        assertTrue(ClassLoaderConfigurer.compareVersions("1.2", "1.1") > 0);
        assertTrue(ClassLoaderConfigurer.compareVersions("1.2.1", "1.2") > 0);
        assertTrue(ClassLoaderConfigurer.compareVersions("1.2-r80", "1.2") < 0);
        assertTrue(ClassLoaderConfigurer.compareVersions("1.2-r82", "1.2-r81") > 0);


        try {
            ClassLoaderConfigurer.compareVersions("1.2-beta", "1.2-alpha");
            fail("Expected exception.");
        } catch (ClassLoaderConfigurer.UncomparableVersionException e) {
            // expected
        }

        try {
            ClassLoaderConfigurer.compareVersions("a", "1");
            fail("Expected exception.");
        } catch (ClassLoaderConfigurer.UncomparableVersionException e) {
            // expected
        }
    }
}

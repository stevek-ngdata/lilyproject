package org.kauriproject.runtime.test;

import junit.framework.TestCase;
import org.kauriproject.runtime.ClassLoaderConfigurer;

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

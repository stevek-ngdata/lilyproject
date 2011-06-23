package org.lilyproject.testfw;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Utility class to create/destroy a random temporary directory under which test utilities
 * can store temporary data.
 */
public class TestHomeUtil {

    public static File createTestHome() throws IOException {
        String randomStr = UUID.randomUUID().toString();
        String tmpdir = System.getProperty("java.io.tmpdir");
        File testHomeDir = new File(tmpdir, "lily-test-" + randomStr);
        FileUtils.forceMkdir(testHomeDir);
        return testHomeDir;
    }

    public static void cleanupTestHome(File testHome) throws IOException {
        FileUtils.deleteDirectory(testHome);
    }

}

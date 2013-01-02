/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.util.test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Utility class to create/destroy a random temporary directory under which test utilities
 * can store temporary data.
 */
public class TestHomeUtil {

    public static File createTestHome() throws IOException {
        return createTestHome("lily-test-");
    }

    public static File createTestHome(String prefix) throws IOException {
        String randomStr = UUID.randomUUID().toString();
        String tmpdir = System.getProperty("java.io.tmpdir");
        File testHomeDir = new File(tmpdir, prefix + randomStr);
        if (!testHomeDir.mkdirs()) {
            throw new IOException("Failed to create directory path " + testHomeDir.getPath());
        }
        return testHomeDir;
    }

    public static void cleanupTestHome(File testHome) throws IOException {
        deleteDirectory(testHome);
    }

    // Not using commons-io on purpose: lily-util project should remain (mostly) dependency free.
    private static void deleteDirectory(File file) throws IOException {
        if (!file.exists()) {
            return;
        }

        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                if (child.isDirectory()) {
                    deleteDirectory(child);
                } else {
                    if (!child.delete()) {
                        throw new IOException("Failed to delete " + child.getAbsolutePath());
                    }
                }
            }
        }

        if (!file.delete()) {
            throw new IOException("Failed to delete " + file.getAbsolutePath());
        }
    }
}

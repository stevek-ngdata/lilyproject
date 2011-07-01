package org.lilyproject.tools.mavenplugin.kauridepresolver;

import java.io.IOException;
import java.util.Properties;

public class KauriMavenUtil {
    public static String getKauriVersion() {
        String kauriVersion;
        try {
            Properties props = new Properties();
            props.load(KauriProjectClasspath.class.getResourceAsStream("kauri.properties"));
            kauriVersion = props.getProperty("kauri.version");
        } catch (IOException e) {
            throw new RuntimeException("Error reading kauri.properties from classpath", e);
        }

        if (kauriVersion == null) {
            throw new RuntimeException("kauri.version property not defined in kauri.properties.");
        }

        return kauriVersion;
    }
}

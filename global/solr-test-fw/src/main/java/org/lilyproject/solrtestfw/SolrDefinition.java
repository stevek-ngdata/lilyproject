package org.lilyproject.solrtestfw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

/**
 * Defines a Solr setup: the schemas, the cores, etc. Always contains at least one core.
 *
 * <p>There is always one core called core0, even if you don't use it. This is because of technical reasons:
 * it is impossible to remove / change the default core. The core0 core is also the default Solr core,
 * thus when using Solr URL's without core name in them, requests will go to the core0 core.</p>
 */
public class SolrDefinition {
    private List<CoreDefinition> cores = new ArrayList<CoreDefinition>();
    public static final String DEFAULT_CORE_NAME = "core0";

    /**
     * Creates a SolrDefinition using the supplied cores. You can create the core specifications using the
     * factory methods like {@link #core(String, byte[], byte[])}.
     *
     * @param cores if no cores are specified, a default core will be created. The first core should be called core0,
     *              if it has a different name, another (dummy) core called core0 will be inserted.
     */
    public SolrDefinition(CoreDefinition... cores) {
        if (cores.length == 0) {
            this.cores.add(core());
        } else {
            if (!cores[0].getName().equals("core0")) {
                this.cores.add(core());
            }
            for (CoreDefinition core : cores) {
                if (nameInUse(core.getName())) {
                    throw new IllegalArgumentException("Double core name: " + core.getName());
                }
                this.cores.add(core);
            }
        }
    }

    public SolrDefinition(byte[] schema, byte[] solrConfig) {
        this.cores.add(core("core0", schema, solrConfig));
    }

    public SolrDefinition(byte[] schema) {
        this.cores.add(core("core0", schema, null));
    }

    private boolean nameInUse(String name) {
        for (CoreDefinition core : cores) {
            if (core.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    public List<CoreDefinition> getCores() {
        return cores;
    }

    public static CoreDefinition core(String name, byte[] schema, byte[] solrConfig) {
        return new CoreDefinition(name, schema, solrConfig);
    }

    public static CoreDefinition core(String name, byte[] schema) {
        return new CoreDefinition(name, schema, null);
    }

    public static CoreDefinition core(String name) {
        return new CoreDefinition(name, null, null);
    }

    /**
     * Creates a core with default schema.xml and solrconfig.xml.
     */
    public static CoreDefinition core() {
        return new CoreDefinition("core0", null, null);
    }

    public static byte[] defaultSolrSchema() {
        try {
            return IOUtils.toByteArray(SolrDefinition.class.getClassLoader()
                    .getResourceAsStream("org/lilyproject/solrtestfw/conftemplate/schema.xml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] defaultSolrConfig() {
        try {
            return IOUtils.toByteArray(SolrDefinition.class.getClassLoader()
                    .getResourceAsStream("org/lilyproject/solrtestfw/conftemplate/solrconfig.xml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class CoreDefinition {
        private String name;
        private byte[] schemaData;
        private byte[] solrConfigData;

        public CoreDefinition(String name, byte[] schemaData, byte[] solrConfigData) {
            this.name = name;
            this.schemaData = schemaData != null ? schemaData : defaultSolrSchema();
            this.solrConfigData = solrConfigData != null ? solrConfigData : defaultSolrConfig();
        }

        public byte[] getSchemaData() {
            return schemaData;
        }

        public byte[] getSolrConfigData() {
            return solrConfigData;
        }

        public String getName() {
            return name;
        }
    }
}

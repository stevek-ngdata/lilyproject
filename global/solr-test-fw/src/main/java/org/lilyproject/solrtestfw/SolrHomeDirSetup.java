package org.lilyproject.solrtestfw;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.NullInputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;

public class SolrHomeDirSetup {
    private File solrHomeDir;
    private SolrDefinition solrDef;
    private String autoCommitSetting;

    private SolrHomeDirSetup(File solrHomeDir, SolrDefinition solrDef, String autoCommitSetting) {
        this.solrHomeDir = solrHomeDir;
        this.solrDef = solrDef;
        this.autoCommitSetting = autoCommitSetting;
    }

    public static void write(File solrHomeDir, SolrDefinition solrDef, String autoCommitSetting) throws IOException {
        new SolrHomeDirSetup(solrHomeDir, solrDef, autoCommitSetting).writeCoreDirs();
    }

    private void writeCoreDirs() throws IOException {
        for (SolrDefinition.CoreDefinition core : solrDef.getCores()) {
            File solrCoreDir = new File(solrHomeDir, core.getName());
            File solrConfDir = new File(solrCoreDir, "conf");
            FileUtils.deleteDirectory(solrCoreDir); // if it would have existed previously
            FileUtils.forceMkdir(solrConfDir);

            copyDefaultConfigToSolrHome(solrConfDir);
            writeSchema(solrConfDir, core.getSchemaData());
            writeSolrConfig(solrConfDir, core.getSolrConfigData());
        }
        writeCoresConf();
    }

    private void writeCoresConf() throws FileNotFoundException {
        File coresFile = new File(solrHomeDir, "solr.xml");
        PrintWriter writer = new PrintWriter(coresFile);
        writer.println("<solr persistent='false'>");
        writer.println(" <cores adminPath='/admin/cores' defaultCoreName='" + SolrDefinition.DEFAULT_CORE_NAME + "'>");
        for (SolrDefinition.CoreDefinition core : solrDef.getCores()) {
            writer.println("  <core name='" + core.getName() + "' instanceDir='" + core.getName() + "'>");
            writer.println("    <property name='solr.data.dir' value='${solr.solr.home}/" + core.getName() + "/data'/>");
            writer.println("  </core>");
        }
        writer.println(" </cores>");
        writer.println("</solr>");
        writer.close();
    }

    private void copyDefaultConfigToSolrHome(File solrConfDir) throws IOException {
        createEmptyFile(new File(solrConfDir, "synonyms.txt"));
        createEmptyFile(new File(solrConfDir, "stopwords.txt"));
        createEmptyFile(new File(solrConfDir, "stopwords_en.txt"));
        createEmptyFile(new File(solrConfDir, "protwords.txt"));
    }

    private void writeSchema(File solrConfDir, byte[] schemaData) throws IOException {
        FileUtils.writeByteArrayToFile(new File(solrConfDir, "schema.xml"), schemaData);
    }

    private void writeSolrConfig(File solrConfDir, byte[] solrConfigData) throws IOException {
        String solrConfigString = new String(solrConfigData, "UTF-8");
        solrConfigString = solrConfigString.replaceAll(Pattern.quote("<!--AUTOCOMMIT_PLACEHOLDER-->"),
                autoCommitSetting == null ? "" : autoCommitSetting);
        FileUtils.writeStringToFile(new File(solrConfDir, "solrconfig.xml"), solrConfigString);
    }

    private void createEmptyFile(File destination) throws IOException {
        FileUtils.copyInputStreamToFile(new NullInputStream(0), destination);
    }
}

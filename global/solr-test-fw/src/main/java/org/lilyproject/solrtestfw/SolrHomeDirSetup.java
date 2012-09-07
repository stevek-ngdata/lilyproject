package org.lilyproject.solrtestfw;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.NullInputStream;

public class SolrHomeDirSetup {
    private final File solrHomeDir;
    private final SolrDefinition solrDef;
    private final String autoCommitSetting;
    private static final String[] SW_LANGS = new String[] {"ar","bg","ca","cz","da","de","el","en","es","eu","fa","fi",
        "fr","ga","gl","hi","hu","hy","id","it","ja","lv","nl","no","pt","ro","ru","sv","th","tr"};
    private static final String[] CONTRACT_LANGS = new String[] {"ca","fr","ga","it"};

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
            // In case the core would have previously existed, delete its directory, except for the default
            // core, since that one is never unloaded, and solr would fail if you delete the index below its feet
            if (!core.getName().equals(SolrDefinition.DEFAULT_CORE_NAME)) {
                FileUtils.deleteDirectory(solrCoreDir);
            }
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
        writer.println("<solr persistent='true'>");
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
        createEmptyFile(new File(solrConfDir, "protwords.txt"));
        for (String lang : SW_LANGS) {
            createEmptyFile(new File(solrConfDir, "lang" + File.separatorChar + "stopwords_" + lang  + ".txt"));
        }
        for (String lang : CONTRACT_LANGS) {
            createEmptyFile(new File(solrConfDir, "lang" + File.separatorChar + "contractions_" + lang  + ".txt"));
        }
        createEmptyFile(new File(solrConfDir, "lang" + File.separatorChar + "hyphenations_ga.txt"));
        createEmptyFile(new File(solrConfDir, "lang" + File.separatorChar + "stoptags_ja.txt"));
        createEmptyFile(new File(solrConfDir, "lang" + File.separatorChar + "stemdict_nl.txt"));
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

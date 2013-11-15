package org.lilyproject.indexer.engine.test;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.util.PropertyPlaceholderHelper;

import java.io.IOException;
import java.util.Properties;

public class IndexerConfWrapper {
    private final static PropertyPlaceholderHelper placeholder = new PropertyPlaceholderHelper("${", "}");

    public static String wrapConf(String indexName, String conf, String repoName, String tableName) throws IOException {
        String template = IOUtils.toString(IndexerConfWrapper.class.getResourceAsStream("hbase-indexer-conf-template.xml"));
        Properties props = new Properties();
        props.setProperty("REPOTABLE", repoName + "__" + tableName);
        props.setProperty("ZOOKEEPER", "localhost:2181");
        props.setProperty("INDEXNAME", indexName);
        props.setProperty("REPOSITORY", repoName);
        props.setProperty("TABLE", tableName);
        props.setProperty("CONF", StringEscapeUtils.escapeXml(conf));

        return placeholder.replacePlaceholders(template, props);
    }
}

package org.lilycms.indexer.test;

import org.junit.Test;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;

import java.io.FileOutputStream;

public class IndexerConfTest {
    @Test
    public void testConf() throws Exception {
        IndexerConf conf = IndexerConfBuilder.build(getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/indexerconf1.xml"));
        FileOutputStream fos = new FileOutputStream("/tmp/schema.xml");
        conf.generateSolrSchema(fos);
        fos.close();
    }
}

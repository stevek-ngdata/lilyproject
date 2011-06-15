package org.lilyproject.tools.import_.json.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.tools.import_.cli.JsonImport;
import org.lilyproject.util.io.Closer;

import static org.junit.Assert.assertEquals;

public class JsonImportTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static Repository repository;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        repository = repoSetup.getRepository();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    /**
     * Tests that namespaces can be declared globally and/or locally.
     */
    @Test
    public void testNamespaceContexts() throws Exception {
        JsonImport.load(repository, getClass().getResourceAsStream("nscontexttest.json"), false);

        Record record1 = repository.read(repository.getIdGenerator().fromString("USER.record1"));
        assertEquals("value1", record1.getField(new QName("import1", "f1")));
        assertEquals(new Integer(55), record1.getField(new QName("import2", "f2")));
    }
}

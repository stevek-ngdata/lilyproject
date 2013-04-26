/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.repository.api.tutorial;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.BlobManagerImpl;
import org.lilyproject.repository.impl.BlobStoreAccessConfig;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseRepositoryManager;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.RecordFactoryImpl;
import org.lilyproject.repository.impl.TableManagerImpl;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.tenant.model.impl.TenantModelImpl;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.PrintUtil;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * The code in this class is used in the repository API tutorial (390-OTC). If this
 * code needs updating because of API changes, then the tutorial itself probably needs
 * to be updated too.
 */
public class TutorialTest {
    private static HBaseProxy HBASE_PROXY;

    private static final String BNS = "book";
    private static final String ANS = "article";

    private static TypeManager typeManager;
    private static RepositoryManager repositoryManager;
    private static Repository repository;
    private static Configuration configuration;
    private static ZooKeeperItf zooKeeper;

    private static HBaseTableFactory hbaseTableFactory;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY = new HBaseProxy();
        HBASE_PROXY.start();

        IdGenerator idGenerator = new IdGeneratorImpl();
        configuration = HBASE_PROXY.getConf();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);
        hbaseTableFactory = new HBaseTableFactoryImpl(HBASE_PROXY.getConf());

        TenantModel tenantModel = new TenantModelImpl(zooKeeper);

        typeManager = new HBaseTypeManager(idGenerator, configuration, zooKeeper, hbaseTableFactory);

        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        List<BlobStoreAccess> blobStoreAccesses = Collections.<BlobStoreAccess>singletonList(dfsBlobStoreAccess);
        BlobStoreAccessConfig blobStoreAccessConfig = new BlobStoreAccessConfig(dfsBlobStoreAccess.getId());
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(blobStoreAccesses, blobStoreAccessConfig);
        BlobManager blobManager = new BlobManagerImpl(hbaseTableFactory, blobStoreAccessFactory, false);
        repositoryManager = new HBaseRepositoryManager(typeManager, idGenerator,
                new RecordFactoryImpl(typeManager, idGenerator), hbaseTableFactory, blobManager, configuration, tenantModel);

        TableManager repoTableManager = new TableManagerImpl(/* TODO multitenancy */ "public", configuration, hbaseTableFactory);
        if (!repoTableManager.tableExists(Table.RECORD.name)) {
            repoTableManager.createTable(Table.RECORD.name);
        }

        repository = repositoryManager.getRepository(Table.RECORD.name);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(typeManager);
        Closer.close(repository);
        Closer.close(zooKeeper);
        Closer.close(repositoryManager);
        HBASE_PROXY.stop();
    }

    @Test
    public void createRecordType() throws Exception {
        // (1)
        ValueType stringValueType = typeManager.getValueType("STRING");

        // (2)
        FieldType title = typeManager.newFieldType(stringValueType, new QName(BNS, "title"), Scope.VERSIONED);

        // (3)
        title = typeManager.createFieldType(title);

        // (4)
        RecordType book = typeManager.newRecordType(new QName(BNS, "Book"));
        book.addFieldTypeEntry(title.getId(), true);

        // (5)
        book = typeManager.createRecordType(book);

        // (6)
        PrintUtil.print(book, repository);
    }

    @Test
    public void updateRecordType() throws Exception {
        FieldType description = typeManager.createFieldType("BLOB", new QName(BNS, "description"), Scope.VERSIONED);
        FieldType authors = typeManager.createFieldType("LIST<STRING>", new QName(BNS, "authors"), Scope.VERSIONED);
        FieldType released = typeManager.createFieldType("DATE", new QName(BNS, "released"), Scope.VERSIONED);
        FieldType pages = typeManager.createFieldType("LONG", new QName(BNS, "pages"), Scope.VERSIONED);
        FieldType sequelTo = typeManager.createFieldType("LINK", new QName(BNS, "sequel_to"), Scope.VERSIONED);
        FieldType manager = typeManager.createFieldType("STRING", new QName(BNS, "manager"), Scope.NON_VERSIONED);
        FieldType reviewStatus = typeManager.createFieldType("STRING", new QName(BNS, "review_status"), Scope.VERSIONED_MUTABLE);

        RecordType book = typeManager.getRecordTypeByName(new QName(BNS, "Book"), null);

        // The order in which fields are added does not matter
        book.addFieldTypeEntry(description.getId(), false);
        book.addFieldTypeEntry(authors.getId(), false);
        book.addFieldTypeEntry(released.getId(), false);
        book.addFieldTypeEntry(pages.getId(), false);
        book.addFieldTypeEntry(sequelTo.getId(), false);
        book.addFieldTypeEntry(manager.getId(), false);
        book.addFieldTypeEntry(reviewStatus.getId(), false);

        // Now we call updateRecordType instead of createRecordType
        book = typeManager.updateRecordType(book);

        PrintUtil.print(book, repository);
    }

    @Test
    public void createRecord() throws Exception {
        // (1)
        Record record = repository.newRecord();

        // (2)
        record.setRecordType(new QName(BNS, "Book"));

        // (3)
        record.setField(new QName(BNS, "title"), "Lily, the definitive guide, 3rd edition");

        // (4)
        record = repository.create(record);

        // (5)
        PrintUtil.print(record, repository);
    }

    @Test
    public void createRecordUserSpecifiedId() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.newRecord(id);
        record.setDefaultNamespace(BNS);
        record.setRecordType("Book");
        record.setField("title", "Lily, the definitive guide, 3rd edition");
        record = repository.create(record);

        PrintUtil.print(record, repository);
    }

    @Test
    public void updateRecord() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.newRecord(id);
        record.setDefaultNamespace(BNS);
        record.setField("title", "Lily, the definitive guide, third edition");
        record.setField("pages", Long.valueOf(912));
        record.setField("manager", "Manager M");
        record = repository.update(record);

        PrintUtil.print(record, repository);
    }

    @Test
    public void updateRecordViaRead() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.read(id);
        record.setDefaultNamespace(BNS);
        record.setField("released", new LocalDate());
        record.setField("authors", Arrays.asList("Author A", "Author B"));
        record.setField("review_status", "reviewed");
        record = repository.update(record);

        PrintUtil.print(record, repository);
    }

    @Test
    public void updateRecordConditionally() throws Exception {
        List<MutationCondition> conditions = new ArrayList<MutationCondition>();
        conditions.add(new MutationCondition(new QName(BNS, "manager"), "Manager Z"));

        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.read(id);
        record.setField(new QName(BNS, "manager"), "Manager P");
        record = repository.update(record, conditions);

        System.out.println(record.getResponseStatus());
    }

    @Test
    public void readRecord() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");


        // (1)
        Record record = repository.read(id);
        String title = (String)record.getField(new QName(BNS, "title"));
        System.out.println(title);

        // (2)
        record = repository.read(id, 1L);
        System.out.println(record.getField(new QName(BNS, "title")));

        // (3)
        record = repository.read(id, 1L, Arrays.asList(new QName(BNS, "title")));
        System.out.println(record.getField(new QName(BNS, "title")));
    }

    @Test
    public void blob() throws Exception {
        //
        // Write a blob
        //

        String description = "<html><body>This book gives thorough insight into Lily, ...</body></html>";
        byte[] descriptionData = description.getBytes("UTF-8");

        // (1)
        Blob blob = new Blob("text/html", (long)descriptionData.length, "description.xml");
        OutputStream os = repository.getOutputStream(blob);
        try {
            os.write(descriptionData);
        } finally {
            os.close();
        }

        // (2)
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.newRecord(id);
        record.setField(new QName(BNS, "description"), blob);
        record = repository.update(record);

        //
        // Read a blob
        //
        InputStream is = null;
        try {
            is = repository.getInputStream(record, new QName(BNS, "description"));
            System.out.println("Data read from blob is:");
            Reader reader = new InputStreamReader(is, "UTF-8");
            char[] buffer = new char[20];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                System.out.print(new String(buffer, 0, read));
            }
            System.out.println();
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    @Test
    public void variantRecord() throws Exception {
        // (1)
        IdGenerator idGenerator = repository.getIdGenerator();
        RecordId masterId = idGenerator.newRecordId();

        // (2)
        Map<String, String> variantProps = new HashMap<String, String>();
        variantProps.put("language", "en");

        // (3)
        RecordId enId = idGenerator.newRecordId(masterId, variantProps);

        // (4)
        Record enRecord = repository.newRecord(enId);
        enRecord.setRecordType(new QName(BNS, "Book"));
        enRecord.setField(new QName(BNS, "title"), "Car maintenance");
        enRecord = repository.create(enRecord);

        // (5)
        RecordId nlId = idGenerator.newRecordId(enRecord.getId().getMaster(), Collections.singletonMap("language", "nl"));
        Record nlRecord = repository.newRecord(nlId);
        nlRecord.setRecordType(new QName(BNS, "Book"));
        nlRecord.setField(new QName(BNS, "title"), "Wagen onderhoud");
        nlRecord = repository.create(nlRecord);

        // (6)
        Set<RecordId> variants = repository.getVariants(masterId);
        for (RecordId variant : variants) {
            System.out.println(variant);
        }
    }

    @Test
    public void linkField() throws Exception {
        // (1)
        Record record1 = repository.newRecord();
        record1.setRecordType(new QName(BNS, "Book"));
        record1.setField(new QName(BNS, "title"), "Fishing 1");
        record1 = repository.create(record1);

        // (2)
        Record record2 = repository.newRecord();
        record2.setRecordType(new QName(BNS, "Book"));
        record2.setField(new QName(BNS, "title"), "Fishing 2");
        record2.setField(new QName(BNS, "sequel_to"), new Link(record1.getId()));
        record2 = repository.create(record2);

        // (3)
        Link sequelToLink = (Link)record2.getField(new QName(BNS, "sequel_to"));
        RecordId sequelTo = sequelToLink.resolve(record2.getId(), repository.getIdGenerator());
        Record linkedRecord = repository.read(sequelTo);
        System.out.println(linkedRecord.getField(new QName(BNS, "title")));
    }

    @Test
    public void complexFields() throws Exception {
        // (1)
        FieldType name = typeManager.createFieldType("STRING", new QName(ANS, "name"), Scope.NON_VERSIONED);
        FieldType email = typeManager.createFieldType("STRING", new QName(ANS, "email"), Scope.NON_VERSIONED);

        RecordType authorType = typeManager.newRecordType(new QName(ANS, "author"));
        authorType.addFieldTypeEntry(name.getId(), true);
        authorType.addFieldTypeEntry(email.getId(), true);
        authorType = typeManager.createRecordType(authorType);

        // (2)
        FieldType title = typeManager.createFieldType("STRING", new QName(ANS, "title"), Scope.NON_VERSIONED);
        FieldType authors = typeManager.createFieldType("LIST<RECORD<{article}author>>",
                new QName(ANS, "authors"), Scope.NON_VERSIONED);
        FieldType body = typeManager.createFieldType("STRING", new QName(ANS, "body"), Scope.NON_VERSIONED);

        RecordType articleType = typeManager.newRecordType(new QName(ANS, "article"));
        articleType.addFieldTypeEntry(title.getId(), true);
        articleType.addFieldTypeEntry(authors.getId(), true);
        articleType.addFieldTypeEntry(body.getId(), true);
        articleType = typeManager.createRecordType(articleType);

        // (3)
        Record author1 = repository.newRecord();
        author1.setRecordType(authorType.getName());
        author1.setField(name.getName(), "Author X");
        author1.setField(name.getName(), "author_x@authors.com");

        Record author2 = repository.newRecord();
        author2.setRecordType(new QName(ANS, "author"));
        author2.setField(name.getName(), "Author Y");
        author2.setField(name.getName(), "author_y@authors.com");

        // (4)
        Record article = repository.newRecord();
        article.setRecordType(articleType.getName());
        article.setField(new QName(ANS, "title"), "Title of the article");
        article.setField(new QName(ANS, "authors"), Lists.newArrayList(author1, author2));
        article.setField(new QName(ANS, "body"), "Body text of the article");
        article = repository.create(article);

        PrintUtil.print(article, repository);
    }

}

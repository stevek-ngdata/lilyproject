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
package org.lilyproject.indexer.hbase.mapper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.google.common.io.ByteStreams;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConfBuilder;
import com.ngdata.hbaseindexer.conf.IndexerConfException;
import com.ngdata.hbaseindexer.conf.IndexerConfReader;
import org.lilyproject.indexer.model.indexerconf.LilyIndexerConfBuilder;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.xml.DocumentHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class LilyIndexerConfReader implements IndexerConfReader {
    @Override
    public IndexerConf read(InputStream is) throws IndexerConfException {
        try {
            byte[] data = ByteStreams.toByteArray(is);

            Document doc = DocumentHelper.parse(new ByteArrayInputStream(data));
            IndexerConfBuilder builder = new IndexerConfBuilder();

            Element indexEl = doc.getDocumentElement();
            String lTable = indexEl.hasAttribute("table") ? indexEl.getAttribute("table") : LilyHBaseSchema.Table.RECORD.name;
            String lRepository = indexEl.hasAttribute("repository") ? indexEl.getAttribute("repository") : RepoAndTableUtil.DEFAULT_REPOSITORY;
            builder.table(RepoAndTableUtil.getHBaseTableName(lRepository, lTable));
            builder.mapperClass(LilyResultToSolrMapper.class);
            builder.mappingType(getEnumAttribute(IndexerConf.MappingType.class, indexEl, "mapping-type", null));
            builder.rowReadMode(getEnumAttribute(IndexerConf.RowReadMode.class, indexEl, "read-row", null));
            String uniqueKey = DocumentHelper.getAttribute(indexEl, "unique-key-field", false);
            if (uniqueKey==null) {
                uniqueKey="lily.key";
            }
            builder.uniqueyKeyField(uniqueKey);
            builder.rowField(DocumentHelper.getAttribute(indexEl, "row-field", false));
            builder.columnFamilyField(DocumentHelper.getAttribute(indexEl, "column-family-field", false));
            builder.tableNameField(DocumentHelper.getAttribute(indexEl, "table-name-field", false));

            // add a copy of the config file so we can parse create a (Lily)IndexerConf later
            builder.globalParams(data);

            return builder.build();
        } catch (Exception e) {
            throw new IndexerConfException("Failed to parse Lily indexer configuration", e);
        }
    }

    @Override
    public void validate(InputStream is) throws IndexerConfException {
        try {
            LilyIndexerConfBuilder.validate(is);
        } catch (org.lilyproject.indexer.model.indexerconf.IndexerConfException e) {
            throw new IndexerConfException(e);
        }
    }

    private <T extends Enum> T getEnumAttribute(Class<T> enumClass, Element element, String attribute, T defaultValue) {
        if (!element.hasAttribute(attribute)) {
            return defaultValue;
        }
        String value = element.getAttribute(attribute);
        try {
            return (T)Enum.valueOf(enumClass, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IndexerConfException("Illegal value in attribute " + attribute + " on element "
                    + element.getLocalName() + ": '" + value);
        }
    }


}

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
package org.lilyproject.indexer.model.util;

import javax.xml.parsers.ParserConfigurationException;

import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import org.lilyproject.indexer.model.api.LResultToSolrMapper;
import org.lilyproject.indexer.model.indexerconf.LilyIndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.indexer.model.indexerconf.LilyIndexerConf;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.util.xml.DocumentHelper;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class IndexInfo {
    IndexerDefinition indexDefinition;
    IndexerConf indexerConf;
    LilyIndexerConf lilyIndexerConf;
    String repositoryName;

    public IndexInfo(IndexerDefinition indexDefinition, IndexerConf indexerConf, RepositoryManager repositoryManager)
            throws IndexerConfException, RepositoryException, InterruptedException {
        this.indexDefinition = indexDefinition;
        this.indexerConf = indexerConf;

        byte[] confData = indexerConf.getGlobalConfig();
        try {
            Document doc = DocumentHelper.parse(new ByteArrayInputStream(confData));
            repositoryName = DocumentHelper.getAttribute(doc.getDocumentElement(), "repository", false);
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        LRepository repository = repositoryName == null ? repositoryManager.getDefaultRepository() : repositoryManager.getRepository(repositoryName);
        repositoryName = repository.getRepositoryName();

        this.lilyIndexerConf = LilyIndexerConfBuilder.build(new ByteArrayInputStream(confData), repository);
    }

    public IndexerDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public IndexerConf getIndexerConf() {
        return indexerConf;
    }

    public LilyIndexerConf getLilyIndexerConf() {
        return lilyIndexerConf;
    }

    public String getRepositoryName() {
        return repositoryName;
    }
}

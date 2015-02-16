/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;

/**
 * Implementation of {@link PercolatorIndex} that can only hold a single Lucene
 * document and is optimized for that
 */
class SingleDocumentPercolatorIndex implements PercolatorIndex {

    private final CloseableThreadLocal<RAMDirectory> cache;

    SingleDocumentPercolatorIndex(CloseableThreadLocal<RAMDirectory> cache) {
        this.cache = cache;
    }

    ESLogger logger = Loggers.getLogger(SingleDocumentPercolatorIndex.class);

    @Override
    public void prepare(PercolateContext context, ParsedDocument parsedDocument) {
        Directory dir = cache.get();
        try {
            IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Version.LATEST, parsedDocument.analyzer()));
            writer.deleteAll();
            writer.addDocument(parsedDocument.rootDoc());
            writer.commit();
            writer.close();
        } catch (IOException ioe) {
            throw new ElasticsearchException("failed to prepare percolator index on ram directory", ioe);
        }
        try {
            context.initialize(new DocEngineSearcher(dir.toString(), new IndexSearcher(DirectoryReader.open(dir))), parsedDocument);
        } catch (IOException ioe) {
            throw new ElasticsearchException("failed to open percolator index on ram directory", ioe);
        }
    }

    private class DocEngineSearcher extends Engine.Searcher {

        public DocEngineSearcher(String source, IndexSearcher searcher) {
            super(source, searcher);
        }

        @Override
        public void close() throws ElasticsearchException {
            try {
                reader().close();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close percolator index on ram directory", e);
            }
        }

    }

}

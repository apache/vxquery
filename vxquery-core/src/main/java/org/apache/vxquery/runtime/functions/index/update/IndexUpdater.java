/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.runtime.functions.index.update;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.index.IndexDocumentBuilder;
import org.apache.vxquery.runtime.functions.index.CaseSensitiveAnalyzer;
import org.apache.vxquery.runtime.functions.index.IndexConstructorUtil;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;

/**
 * Update the index if the source files are changed.
 */
public class IndexUpdater {
    private MetaFileUtil metaFileUtil;
    private ConcurrentHashMap<String, XmlMetadata> metadataMap;
    private IPointable result;
    private final SequenceBuilder sb = new SequenceBuilder();
    private ArrayBackedValueStorage abvs;
    private ITreeNodeIdProvider nodeIdProvider;
    private ArrayBackedValueStorage abvsFileNode;
    private String nodeId;
    private IndexWriter indexWriter;
    private Set<String> pathsFromFileList;
    private String collectionFolder;
    private String indexFolder;
    private final Logger LOGGER = Logger.getLogger("Index Updater");
    private SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private IndexConstructorUtil indexConstructorUtil = new IndexConstructorUtil();

    public IndexUpdater(String indexFolder, IPointable result, ArrayBackedValueStorage abvs, ITreeNodeIdProvider nodeIdProvider,
            ArrayBackedValueStorage abvsFileNode, String nodeId) {
        this.indexFolder = indexFolder;
        this.result = result;
        this.abvs = abvs;
        this.nodeIdProvider = nodeIdProvider;
        this.abvsFileNode = abvsFileNode;
        this.nodeId = nodeId;
        this.pathsFromFileList = new HashSet<>();
    }

    /**
     * Perform the initial configuration for index update/ delete processes.
     *
     * @throws IOException
     *             : If getting the index folder generates {@link IOException}
     */
    public void setup() throws IOException {
        // Read the metadata file and load the metadata map into memory.
        metaFileUtil = new MetaFileUtil(indexFolder);
        metaFileUtil.readMetadataFile();
        metadataMap = metaFileUtil.getMetadata();

        // Retrieve the collection folder path.
        // Remove the entry for ease of the next steps.
        collectionFolder = metaFileUtil.getCollection();

        abvs.reset();
        sb.reset(abvs);

        Directory fsdir = FSDirectory.open(Paths.get(indexFolder));
        indexWriter = new IndexWriter(fsdir, new IndexWriterConfig(new CaseSensitiveAnalyzer())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND));
    }

    /**
     * Wrapper for update index function.
     *
     * @throws IOException
     *             : If the directory doesn't exist
     */
    public void updateIndex() throws IOException {
        File collectionDirectory = new File(collectionFolder);
        if (!collectionDirectory.exists()) {
            throw new IOException("The collection directory (" + collectionFolder + ") does not exist.");
        }

        //Execute update index process
        updateIndex(collectionDirectory);

        //Detect deleted files and execute the delete index process.
        deleteIndexOfDeletedFiles(metadataMap.keySet(), pathsFromFileList);
        updateMetadataFile();
    }

    /**
     * Close opened IndexWriter and terminate the index update/ delete process.
     *
     * @throws IOException
     *             : If exiting the index folder generates {@link IOException}
     */
    public void exit() throws IOException {
        indexWriter.forceMerge(1);

        indexWriter.close();

        sb.finish();
        result.set(abvs);
    }

    /**
     * Functional wrapper to update Metadata file.
     *
     * @throws IOException
     *             : If updating metadata folder generates {@link IOException}
     */
    public synchronized void updateMetadataFile() throws IOException {
        //Write the updated metadata to the file.
        metaFileUtil.updateMetadataMap(metadataMap, indexFolder);
        metaFileUtil.writeMetadataToFile();
    }

    /**
     * Check the collection for changes.
     * If changes are detected, update the index
     *
     * @param collection
     *            : Collection folder path
     */
    private void updateIndex(File collection) throws IOException {

        File[] list = collection.listFiles();

        assert list != null;
        for (File file : list) {
            pathsFromFileList.add(file.getCanonicalPath());
            if (indexConstructorUtil.readableXmlFile(file.getCanonicalPath())) {
                XmlMetadata data = metadataMap.get(file.getCanonicalPath());
                String md5 = metaFileUtil.generateMD5(file);

                abvsFileNode.reset();

                IndexDocumentBuilder indexDocumentBuilder;
                if (data != null) {

                    // This case checks whether the file has been changed.
                    // If the file has changed, delete the existing document, create a new index document and add it
                    // to the current index.
                    // At the same time, update the metadata for the file.
                    if (!md5.equals(data.getMd5())) {

                        //Update index corresponding to the xml file.
                        indexWriter.deleteDocuments(new Term(Constants.FIELD_PATH, file.getCanonicalPath()));
                        indexDocumentBuilder = indexConstructorUtil.getIndexBuilder(file, indexWriter, abvsFileNode,
                                nodeIdProvider, nodeId);
                        indexDocumentBuilder.printStart();

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.log(Level.DEBUG, "New Index is created for updated file " + file.getCanonicalPath());
                        }

                        //Update the metadata map.
                        XmlMetadata metadata = updateEntry(file, data);
                        metadataMap.replace(file.getCanonicalPath(), metadata);

                    }
                } else {

                    // In this case, the xml file has not added to the index. (It is a newly added file)
                    // Therefore generate a new index for this file and add it to the existing index.
                    indexDocumentBuilder = indexConstructorUtil.getIndexBuilder(file, indexWriter, abvsFileNode, nodeIdProvider,
                            nodeId);
                    indexDocumentBuilder.printStart();

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, "New Index is created for newly added file " + file.getCanonicalPath());
                    }

                    XmlMetadata metadata = updateEntry(file, null);
                    metadataMap.put(file.getCanonicalPath(), metadata);
                }
            } else if (file.isDirectory()) {
                updateIndex(file);
            }
        }
    }

    /**
     * Update the current XmlMetadata object related to the currently reading XML file.
     *
     * @param file
     *            : XML file
     * @param metadata
     *            : Existing metadata object
     * @return : XML metadata object with updated fields.
     * @throws IOException
     *             : If getting the file info generates {@link IOException}
     */
    private XmlMetadata updateEntry(File file, XmlMetadata metadataArg) throws IOException {
        XmlMetadata metadata = metadataArg;
        if (metadata == null) {
            metadata = new XmlMetadata();
        }
        metadata.setFileName(file.getName());
        metadata.setPath(file.getCanonicalPath());
        metadata.setMd5(metaFileUtil.generateMD5(file));
        metadata.setLastModified(sdf.format(file.lastModified()));
        return metadata;
    }

    /**
     * Delete the index of deleted files.
     *
     * @param pathsFromMap
     *            : Set of paths taken from metafile.
     * @param pathsFromFileList
     *            : Set of paths taken from list of existing files.
     * @throws IOException
     *             : If deleting Documents generates {@link IOException}
     */
    private void deleteIndexOfDeletedFiles(Set<String> pathsFromMap, Set<String> pathsFromFileList) throws IOException {
        Set<String> sfm = new HashSet<>(pathsFromMap);

        // If any file has been deleted from the collection, the number of files stored in metadata is higher  than
        // the actual number of files.
        // With set difference, the paths of deleted files are taken from the stored metadata.
        // Delete the corresponding indexes of each file from the index and as well as remove the entry from the
        // metadata file.

        if (sfm.size() > pathsFromFileList.size()) {
            sfm.removeAll(pathsFromFileList);

            for (String s : sfm) {
                metadataMap.remove(s);
                indexWriter.deleteDocuments(new Term(Constants.FIELD_PATH, s));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, "Index of the deleted file " + s + " was deleted from the index!");
                }
            }
        }
    }

    /**
     * Delete all indexes in the given directory.
     * This will also remove the existing metadata file.
     * It will be created when recreating the index.
     * When deleting indexes, if any error occurred, the process will be rolled back and all the indexes will be
     * restored.
     * Otherwise the changes will be committed.
     *
     * @throws SystemException
     *             : An attempt to divide by zero
     */
    public void deleteAllIndexes() throws SystemException {
        try {
            indexWriter.deleteAll();
            indexWriter.commit();
            indexWriter.close();
            metaFileUtil.deleteMetaDataFile();

            for (File f : (new File(indexFolder)).listFiles())
                Files.delete(f.toPath());

            sb.finish();
            result.set(abvs);
        } catch (IOException e) {
            try {
                indexWriter.rollback();
                indexWriter.close();

                sb.finish();
                result.set(abvs);
            } catch (IOException e1) {
                throw new SystemException(ErrorCode.FOAR0001, e1);
            }
        }

    }

}

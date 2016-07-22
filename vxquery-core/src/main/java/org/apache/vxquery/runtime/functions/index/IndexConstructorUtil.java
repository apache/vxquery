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
package org.apache.vxquery.runtime.functions.index;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.index.IndexDocumentBuilder;
import org.apache.vxquery.runtime.functions.index.updateIndex.MetaFileUtil;
import org.apache.vxquery.runtime.functions.index.updateIndex.XmlMetadata;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.xmlparser.IParser;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.XMLParser;

import javax.xml.bind.JAXBException;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class IndexConstructorUtil {
    static boolean isMetaFilePresent = false;
    static MetaFileUtil metaFileUtil;
    static ConcurrentHashMap<String, XmlMetadata> metadataMap = new ConcurrentHashMap<>();

    public static void evaluate(String collectioFolder, String indexFolder, IPointable result, UTF8StringPointable
            stringp,
            ByteBufferInputStream bbis, DataInputStream di, SequenceBuilder sb, ArrayBackedValueStorage abvs,
            ITreeNodeIdProvider nodeIdProvider, ArrayBackedValueStorage abvsFileNode, TaggedValuePointable nodep,
            boolean isElementPath, String nodeId) throws SystemException, JAXBException {
        //        TaggedValuePointable collectionTVP = args[0];
//        TaggedValuePointable indexTVP = args[1];
//
//        if (collectionTVP.getTag() != ValueTag.XS_STRING_TAG || indexTVP.getTag() != ValueTag.XS_STRING_TAG) {
//            throw new SystemException(ErrorCode.FORG0006);
//        }

//        try {
            // Get the list of files.
//            collectionTVP.getValue(stringp);
//            bbis.setByteBuffer(ByteBuffer.wrap(Arrays.copyOfRange(stringp.getByteArray(), stringp.getStartOffset(),
//                    stringp.getLength() + stringp.getStartOffset())), 0);
//            collectionFolder = di.readUTF();
//
//            // Get the index folder
//            indexTVP.getValue(stringp);
//            bbis.setByteBuffer(ByteBuffer.wrap(Arrays.copyOfRange(stringp.getByteArray(), stringp.getStartOffset(),
//                    stringp.getLength() + stringp.getStartOffset())), 0);
//            indexFolder = di.readUTF();

            metaFileUtil = MetaFileUtil.create(indexFolder);
            isMetaFilePresent = metaFileUtil.isMetaFilePresent();
            metaFileUtil.setCollectionForIndex(indexFolder, collectioFolder);
//
//        } catch (IOException e) {
//            throw new SystemException(ErrorCode.SYSE0001, e);
//        }
        File collectionDirectory = new File(collectioFolder);
        if (!collectionDirectory.exists()) {
            throw new RuntimeException("The collection directory (" + collectioFolder + ") does not exist.");
        }

        try {
            abvs.reset();
            sb.reset(abvs);

            Directory dir = FSDirectory.open(Paths.get(indexFolder));
            Analyzer analyzer = new CaseSensitiveAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Create will overwrite the index everytime
            iwc.setOpenMode(OpenMode.CREATE);

            //Create an index writer
            IndexWriter writer = new IndexWriter(dir, iwc);

            //Add files to index
            indexXmlFiles(collectionDirectory, writer, isElementPath, nodep, abvsFileNode, nodeIdProvider, sb, bbis, di,
                    nodeId);

            if (!isMetaFilePresent) {
                // Write metadata map to a file.
                metaFileUtil.updateMetadataMap(metadataMap, indexFolder);
                metaFileUtil.writeMetadataToFile();
            }

            //This makes write slower but search faster.
            writer.forceMerge(1);

            writer.close();

            sb.finish();
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    /*This function goes recursively one file at a time. First it turns the file into an ABVS document node, then
     * it indexes that document node.
     */
    public static void indexXmlFiles(File collectionDirectory, IndexWriter writer, boolean isElementPath,
            TaggedValuePointable nodep, ArrayBackedValueStorage abvsFileNode, ITreeNodeIdProvider nodeIdProvider,
            SequenceBuilder sb, ByteBufferInputStream bbis, DataInputStream di, String nodeId)
            throws SystemException, IOException {

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy, HH:mm:ss");

        for (File file : collectionDirectory.listFiles()) {

            if (readableXmlFile(file.getPath())) {
                abvsFileNode.reset();

                IndexDocumentBuilder ibuilder = getIndexBuilder(file, writer, nodep, abvsFileNode, nodeIdProvider, bbis,
                        di, nodeId);

                ibuilder.printStart();
                if (!isMetaFilePresent) {
                    XmlMetadata xmlMetadata = new XmlMetadata();
                    xmlMetadata.setPath(file.getCanonicalPath());
                    xmlMetadata.setFileName(file.getName());
                    xmlMetadata.setLastModified(sdf.format(file.lastModified()));
                    try {
                        xmlMetadata.setMd5(metaFileUtil.generateMD5(file));
                    } catch (NoSuchAlgorithmException e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                    metadataMap.put(file.getCanonicalPath(), xmlMetadata);
                }

            } else if (file.isDirectory()) {
                // Consider all XML file in sub directories.
                indexXmlFiles(file, writer, isElementPath, nodep, abvsFileNode, nodeIdProvider, sb, bbis, di, nodeId);
            }
        }
    }

    public static boolean readableXmlFile(String path) {
        return (path.toLowerCase().endsWith(".xml") || path.toLowerCase().endsWith(".xml.gz"));
    }

    /**
     * Separated from create index method so that it could be used as a helper function in IndexUpdater
     */
    public static IndexDocumentBuilder getIndexBuilder(File file, IndexWriter writer, TaggedValuePointable nodep,
            ArrayBackedValueStorage abvsFileNode, ITreeNodeIdProvider nodeIdProvider, ByteBufferInputStream bbis,
            DataInputStream di, String nodeId) throws IOException {

        //Get the document node
        IParser parser = new XMLParser(false, nodeIdProvider, nodeId);
        FunctionHelper.readInDocFromString(file.getPath(), bbis, di, abvsFileNode, parser);

        nodep.set(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(), abvsFileNode.getLength());

        //Add the document to the index
        //Creates one lucene doc per file
        return new IndexDocumentBuilder(nodep, writer, file.getCanonicalPath());
    }
}

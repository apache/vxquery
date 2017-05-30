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
package org.apache.vxquery.runtime.functions.index.centralizer;

import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;

/**
 * Class for maintaining the centralized index information file.
 * Index centralization procedure.
 * User can specify the collection directory in VXQuery.java, ncConfig.ioDevices = &lt; index_directory &gt; .
 * Then all the indexes will be created in that particular directory in sub-folders corresponding to collections.
 * There will be a single xml file, located in the directory specified in local.xml, which contains all information
 * about the existing indexes.
 * This class can be used to read, add, delete, modify the entries and write the file back to the disk.
 */
public class IndexCentralizerUtil {

    private static final String FILE_NAME = "VXQuery-Index-Directory.xml";
    private final List<String> collections = new ArrayList<>();
    private static final Logger LOGGER = Logger.getLogger("IndexCentralizerUtil");
    private File xmlFile;
    private String indexPath;
    public static ConcurrentHashMap<String, IndexLocator> indexCollectionMap = new ConcurrentHashMap<>();
    private static final StringValueBuilder svb = new StringValueBuilder();
    private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private final DataOutput output = abvs.getDataOutput();

    public IndexCentralizerUtil(File index) {
        indexPath = index.getPath();
        if (!index.exists()) {
            try {
                FileUtils.forceMkdir(index);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Could not create the index directory for path: " + indexPath + " " + e);
            }
        }
        xmlFile = new File(index.getPath() + "/" + FILE_NAME);
    }

    /**
     * Get the index directory containing index of the given collection
     *
     * @param collection
     *            : Collection folder
     * @return Index folder.
     */
    public String getIndexForCollection(String collection) {
        if (indexCollectionMap.size() > 0 && indexCollectionMap.containsKey(collection)) {
            return indexCollectionMap.get(collection).getIndex();
        }
        return null;
    }

    /**
     * Put the index location corresponding to given collection.
     * Index location is created by using the last 100 characters of collection.
     *
     * @param collection
     *            : Collection directory
     * @return index
     */
    public String putIndexForCollection(String collection) {
        int length = collection.replaceAll("/", "").length();
        String index = collection.replaceAll("/", "");
        index = indexPath + "/" + (length > 100 ? index.substring(length - 100) : index);
        IndexLocator il = new IndexLocator();
        il.setCollection(collection);
        il.setIndex(index);
        if (indexCollectionMap.get(collection) != null) {
            return index;
        }
        indexCollectionMap.put(collection, il);
        return index;
    }

    /**
     * Remove the entry for given collection directory.
     *
     * @param collection
     *            : Collection directory
     */
    public void deleteEntryForCollection(String collection) {
        indexCollectionMap.remove(collection);
    }

    /**
     * Prints all collections which have an index created.
     *
     * @param sb
     *            : The output is stored in a sequence
     * @throws IOException
     *             : If writing the dataOutput generates {@link IOException}
     */
    public void getAllCollections(SequenceBuilder sb) throws IOException {
        for (String s : collections) {
            abvs.reset();
            output.write(ValueTag.XS_STRING_TAG);
            svb.write(s, output);
            sb.addItem(abvs);
        }
    }

    /**
     * Read the collection, index directory file and populate the HashMap.
     */
    public void readIndexDirectory() {
        if (xmlFile.exists()) {
            try {
                JAXBContext jaxbContext = JAXBContext.newInstance(IndexDirectory.class);
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                IndexDirectory indexDirectory = (IndexDirectory) jaxbUnmarshaller.unmarshal(xmlFile);

                for (IndexLocator il : indexDirectory.getDirectory()) {
                    indexCollectionMap.put(il.getCollection(), il);
                    this.collections.add(il.getCollection());
                }
            } catch (JAXBException e) {
                LOGGER.log(Level.SEVERE, "Could not read the XML file due to " + e);
            }
        }

    }

    /**
     * Write back the contents of the HashMap to the file.
     */
    public void writeIndexDirectory() {
        IndexDirectory id = new IndexDirectory();
        List<IndexLocator> indexLocators = new ArrayList<>(indexCollectionMap.values());
        id.setDirectory(indexLocators);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(this.xmlFile);
            JAXBContext context = JAXBContext.newInstance(IndexDirectory.class);
            Marshaller jaxbMarshaller = context.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            jaxbMarshaller.marshal(id, fileOutputStream);
        } catch (JAXBException | FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Could not read the XML file due to " + e);
        }
    }
}

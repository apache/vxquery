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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Utility class for writing, reading metadata file and generating checksum.
 */
public class MetaFileUtil {

    private File metaFile;
    private static final Logger LOGGER = Logger.getLogger("MetadataFileUtil");
    private String index;
    private String collection;
    private ConcurrentHashMap<String, XmlMetadata> indexMap = new ConcurrentHashMap<>();

    public MetaFileUtil(String indexFolder) {
        this.metaFile = new File(indexFolder + "/" + Constants.META_FILE_NAME);
    }

    /**
     * Checks for existing metadata file.
     *
     * @return true if the metadata file is present
     */
    public boolean isMetaFilePresent() {
        return metaFile.exists();
    }

    /**
     * Update the content of the metadata map.
     * If the current collection data is present, replace it.
     * Otherwise insert new.
     *
     * @param metadataMap
     *            : Set of XmlMetaData objects.
     * @param index
     *            : The path to index location.
     */
    public void updateMetadataMap(ConcurrentHashMap<String, XmlMetadata> metadataMap, String index) {
        this.indexMap = metadataMap;
        this.index = index;

    }

    /**
     * Method to get the set of xml metadata for a given collection
     *
     * @return : Map containing the set of XmlMetadata objects.\
     */
    public ConcurrentHashMap<String, XmlMetadata> getMetadata() {
        return this.indexMap;
    }

    /**
     * Read the metadata file and create an in-memory map containing collection paths and xml files.
     */
    public void readMetadataFile() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(VXQueryIndex.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            XmlMetadataCollection indexes = (XmlMetadataCollection) jaxbUnmarshaller.unmarshal(metaFile);

            this.collection = indexes.getCollection();
            this.index = indexes.getIndexLocation();

            for (XmlMetadata metadata : indexes.getMetadataList()) {
                this.indexMap.put(index, metadata);
            }
        } catch (JAXBException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.log(Level.ERROR, "Could not read the XML file due to " + e);
            }
        }
    }

    /**
     * Write the content of the ConcurrentHashMap to the xml metadata file.
     */
    public void writeMetadataToFile() {
        XmlMetadataCollection xmlMetadataCollection = new XmlMetadataCollection();
        List<XmlMetadata> metadataList = new ArrayList<>();

        for (Map.Entry<String, XmlMetadata> entry : this.indexMap.entrySet()) {
            metadataList.add(entry.getValue());
        }

        xmlMetadataCollection.setMetadataList(metadataList);
        xmlMetadataCollection.setCollection(collection);
        xmlMetadataCollection.setIndexLocation(this.index);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(this.metaFile);
            JAXBContext jaxbContext = JAXBContext.newInstance(VXQueryIndex.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            jaxbMarshaller.marshal(xmlMetadataCollection, fileOutputStream);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.log(Level.DEBUG, "Writing metadata file completed successfully!");
            }
        } catch (JAXBException | FileNotFoundException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.log(Level.ERROR, "Could not read the XML file due to " + e);
            }
        }

    }

    /**
     * Generate MD5 checksum string for a given file.
     *
     * @param file
     *            : File which the checksum should be generated.
     * @return : Checksum String
     * @throws IOException
     *             : The file is not available
     */
    public String generateMD5(File file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(Files.readAllBytes(file.toPath()));
            byte[] md5 = md.digest();
            return DatatypeConverter.printHexBinary(md5);
        } catch (NoSuchAlgorithmException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.log(Level.ERROR, "No Such Algorithm Error " + e.getMessage());
            }
            return null;
        }
    }

    /**
     * Delete the existing Metadata file.
     *
     * @return True if deleted, false otherwise.
     */
    public boolean deleteMetaDataFile() {
        try {
            Files.delete(Paths.get(metaFile.getCanonicalPath()));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.log(Level.DEBUG, "Metadata file deleted!");
            }
            return true;
        } catch (IOException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.log(Level.ERROR, "Metadata file could not be deleted!");
            }
            return false;
        }
    }

    /**
     * Get the collection for a given index location.
     *
     * @return collection folder for a given index.
     */
    public String getCollection() {
        return this.collection;
    }

    /**
     * Set the entry for given index and collection.
     *
     * @param collection
     *            : path to corresponding collection
     */
    public void setCollection(String collection) {
        this.collection = collection;
    }
}

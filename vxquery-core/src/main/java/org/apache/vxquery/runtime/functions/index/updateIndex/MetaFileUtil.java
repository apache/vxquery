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
package org.apache.vxquery.runtime.functions.index.updateIndex;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for writing, reading metadata file and generating checksum.
 */
public class MetaFileUtil {

    private File metaFile;
    private Logger LOGGER = Logger.getLogger("MetadataFileUtil");

    private MetaFileUtil(String indexFolder) {
        this.metaFile = new File(indexFolder + "/" + Constants.META_FILE_NAME);
    }

    public static MetaFileUtil create(String indexFolder) {
        return new MetaFileUtil(indexFolder);
    }

    /**
     * Checks for existing metadata file.
     * @return true if the metadata file is present
     */
    public boolean isMetaFilePresent() {
        return metaFile.exists();
    }

    /**
     * Write the given List of XmlMetadata objects to a file.
     * If the metadata file is already presents, delete it.
     *
     * @param metadataMap : Set of XmlMetaData objects
     * @throws IOException
     */
    public void writeMetaFile(ConcurrentHashMap<String, XmlMetadata> metadataMap) throws IOException {
        if (this.isMetaFilePresent()) Files.delete(Paths.get(metaFile.getCanonicalPath()));

        FileOutputStream fileOutputStream = new FileOutputStream(this.metaFile);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(metadataMap);
        objectOutputStream.close();

        if (LOGGER.isDebugEnabled())
            LOGGER.log(Level.DEBUG, "Writing metadata file completed successfully!");

    }


    /**
     * Read metadata file
     *
     * @return : List of XmlMetadata objects
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public ConcurrentHashMap<String, XmlMetadata> readMetaFile() throws IOException, ClassNotFoundException {
        FileInputStream fin = new FileInputStream(this.metaFile);
        ObjectInputStream ois = new ObjectInputStream(fin);
        ConcurrentHashMap<String, XmlMetadata> metadataMap = new ConcurrentHashMap<>((Map<String, XmlMetadata>)ois
                .readObject()) ;
        ois.close();

        return metadataMap;

    }

    /**
     * Generate MD5 checksum string for a given file.
     *
     * @param file : File which the checksum should be generated.
     * @return : Checksum String
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    public String generateMD5(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(Files.readAllBytes(file.toPath()));
        byte[] md5 = md.digest();
        return DatatypeConverter.printHexBinary(md5);
    }

    /**
     * Delete the existing Metadata file.
     * @return True if deleted, false otherwise.
     */
    public boolean deleteMetaDataFile() {
        try {
            Files.delete(Paths.get(metaFile.getCanonicalPath()));
            LOGGER.log(Level.DEBUG, "Metadata file deleted!");
            return true;
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Metadata file could not be deleted!");
            return false;
        }
    }

}

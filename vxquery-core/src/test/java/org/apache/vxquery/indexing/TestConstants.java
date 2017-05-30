/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.indexing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.vxquery.runtime.functions.index.update.XmlMetadata;

/**
 * TestConstants and methods which will be used in indexing test cases.
 */
public class TestConstants {
    public static String INITIAL_MD5 = "F62EE4BBBBE37183E5F50BB1A0B4FFB4";
    public static String CHANGED_MD5 = "98B31970B863E86AB2D7852B346FF234";

    public static String COLLECTION = "src/test/resources/collection/";
    public static String XML_FILE = "/tmp/index/catalog.xml";

    public static String INDEX_DIR = "/tmp/index";

    private static ConcurrentHashMap<String, XmlMetadata> initialMetadataMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, XmlMetadata> modifiedMetadataMap = new ConcurrentHashMap<>();

    /**
     * Creates a HashMap with initial sample data and returns it.
     *
     * @return HashMap with sample data.
     */
    public static ConcurrentHashMap<String, XmlMetadata> getInitialMap() {
        XmlMetadata metadata = new XmlMetadata();
        metadata.setFileName("catalog.xml");
        metadata.setPath(XML_FILE);
        metadata.setMd5(INITIAL_MD5);
        initialMetadataMap.put(XML_FILE, metadata);

        return initialMetadataMap;
    }

    /**
     * Creates a HashMap with modified data and returns it.
     *
     * @return HashMap with sample data.
     */
    public static ConcurrentHashMap<String, XmlMetadata> getModifiedMap() {
        XmlMetadata metadata = new XmlMetadata();
        metadata.setFileName("catalog.xml");
        metadata.setPath(XML_FILE);
        metadata.setMd5(CHANGED_MD5);
        modifiedMetadataMap.put(XML_FILE, metadata);

        return modifiedMetadataMap;
    }

    /**
     * Generate XML file from given template.
     *
     * @param fileName : Template file name
     * @throws IOException
     */
    public static void createXML(String fileName) throws IOException {
        File collectionDir = new File(COLLECTION);

        String src = collectionDir.getCanonicalPath() + File.separator + fileName;
        String dest = INDEX_DIR + File.separator + "catalog.xml";

        //Delete any existing file
        Files.deleteIfExists(Paths.get(dest));

        File in = new File(src);
        FileInputStream fileInputStream = new FileInputStream(in);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));

        FileWriter writer = new FileWriter(dest, true);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        String line;

        while ((line = reader.readLine()) != null) {
            bufferedWriter.write(line);
            bufferedWriter.newLine();
        }

        reader.close();

        bufferedWriter.close();
    }

    /**
     * Get the XmlMetadata contents as an String.
     *
     * @param metadata : XmlMetadata Object
     * @return String containing metadata
     */
    public static String getXMLMetadataString(XmlMetadata metadata) {
        return String.format("%s %s %s", metadata.getFileName(), metadata.getPath(), metadata.getMd5());
    }

}

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

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.vxquery.runtime.functions.index.update.MetaFileUtil;
import org.apache.vxquery.runtime.functions.index.update.XmlMetadata;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test cases for testing MetaFileUtil functions.
 * 1) Creating MetaData file
 * 2) Generating MD5 Hashes
 * 3) Detecting file changes
 * 4) Updating metadata
 * 5) Delete metadata file
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MetaFileUtilTest {

    private static MetaFileUtil metaFileUtil;
    private static ConcurrentHashMap<String, XmlMetadata> initialMap;
    private static ConcurrentHashMap<String, XmlMetadata> modifiedMap;

    @BeforeClass
    public static void setup() {
        new File(TestConstants.INDEX_DIR).mkdir();
        metaFileUtil = new MetaFileUtil(TestConstants.INDEX_DIR);
        initialMap = TestConstants.getInitialMap();
        modifiedMap = TestConstants.getModifiedMap();
    }

    /**
     * Test case for generating MD5 string for an XML file.
     */
    @Test
    public void step1_testGenerateMD5ForXML() throws IOException {
        TestConstants.createXML("catalog.xml");
        File xml = new File(TestConstants.XML_FILE);
        String md5 = metaFileUtil.generateMD5(xml);

        Assert.assertEquals(TestConstants.INITIAL_MD5, md5);

    }

    /**
     * Test the creation of metadata file.
     */
    @Test
    public void step2_testCreateMetaDataFile() {
        ConcurrentHashMap<String, XmlMetadata> initialMap = TestConstants.getInitialMap();
        metaFileUtil.updateMetadataMap(initialMap, "");
        metaFileUtil.writeMetadataToFile();
        Assert.assertEquals(true, metaFileUtil.isMetaFilePresent());
    }

    /**
     * Validate the content of the file.
     */
    @Test
    public void step3_testValidateMetadataFile() {
        ConcurrentHashMap<String, XmlMetadata> fromFile = metaFileUtil.getMetadata();
        Set<String> from = fromFile.keySet();
        Set<String> initial = initialMap.keySet();

        Assert.assertTrue(from.containsAll(initial));

        for (String key : initial) {
            Assert.assertEquals(TestConstants.getXMLMetadataString(initialMap.get(key)),
                    TestConstants.getXMLMetadataString(fromFile.get(key)));
        }

    }

    /**
     * Change the xml file and test whether the changes are detected.
     */
    @Test
    public void step4_testDetectFileChanges() throws IOException {
        TestConstants.createXML("catalog_edited.xml");
        File xml = new File(TestConstants.XML_FILE);
        Assert.assertTrue(metaFileUtil.generateMD5(xml).equals(TestConstants.CHANGED_MD5));
    }

    /**
     * Test the update metadata file process.
     */
    @Test
    public void step5_testUpdateMetadata()
            throws IOException, ClassNotFoundException, NoSuchAlgorithmException, JAXBException {
        ConcurrentHashMap<String, XmlMetadata> fromFileMap = metaFileUtil.getMetadata();
        XmlMetadata modified = fromFileMap.get(TestConstants.XML_FILE);

        File xml = new File(TestConstants.XML_FILE);
        modified.setMd5(metaFileUtil.generateMD5(xml));

        fromFileMap.replace(TestConstants.XML_FILE, modified);

        metaFileUtil.updateMetadataMap(fromFileMap, TestConstants.INDEX_DIR);

        Assert.assertNotNull(metaFileUtil.getMetadata());

    }

    /**
     * Validate the updated metadata.
     */
    @Test
    public void step6_testVerifyMetadataChange() {
        ConcurrentHashMap<String, XmlMetadata> fromFile = metaFileUtil.getMetadata();
        Set<String> from = fromFile.keySet();
        Set<String> modified = modifiedMap.keySet();

        Assert.assertTrue(from.containsAll(modified));

        for (String key : modified) {
            Assert.assertEquals(TestConstants.getXMLMetadataString(modifiedMap.get(key)),
                    TestConstants.getXMLMetadataString(fromFile.get(key)));
        }
    }

    /**
     * Test deletion of metadata file
     */
    @Test
    public void step7_testDeleteMetadataFile() {
        metaFileUtil.deleteMetaDataFile();
        Assert.assertFalse(metaFileUtil.isMetaFilePresent());
    }

    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.forceDelete(new File(TestConstants.INDEX_DIR));
    }
}

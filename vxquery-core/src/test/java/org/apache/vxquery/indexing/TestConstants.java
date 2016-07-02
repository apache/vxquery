package org.apache.vxquery.indexing;

import org.apache.vxquery.runtime.functions.index.updateIndex.XmlMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TestConstants and methods which will be used in indexing test cases.
 */
public class TestConstants {
    public static String INITIAL_MD5 = "16E867C6F082E04D04FB2EF7831647AD";
    public static String CHANGED_MD5 = "49329F554B2F8A81D8F79D2EC4BC3A98";

    public static String COLLECTION = "src/test/resources/collection/";
    public static String XML_FILE = "/tmp/index/catalog.xml";
    public static String META_FILE_NAME = "metaFile.file";

    public static String INDEX_DIR = "/tmp/index";


    private static ConcurrentHashMap<String, XmlMetadata> initialMetadataMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, XmlMetadata> modifiedMetadataMap = new ConcurrentHashMap<>();

    /**
     * Creates a HashMap with initial sample data and returns it.
     * @return HashMap with sample data.
     */
    public static ConcurrentHashMap<String, XmlMetadata> getInitialMap() {
        XmlMetadata metadata = new XmlMetadata();
        metadata.setFileName("catalog.xml");
        metadata.setPath(XML_FILE);
        metadata.setMd5(INITIAL_MD5);

        XmlMetadata collection = new XmlMetadata();
        collection.setPath(COLLECTION);

        initialMetadataMap.put(COLLECTION, collection);
        initialMetadataMap.put(XML_FILE, metadata);

        return initialMetadataMap;
    }

    /**
     * Creates a HashMap with modified data and returns it.
     * @return HashMap with sample data.
     */
    public static ConcurrentHashMap<String, XmlMetadata> getModifiedMap() {
        XmlMetadata metadata = new XmlMetadata();
        metadata.setFileName("catalog.xml");
        metadata.setPath(XML_FILE);
        metadata.setMd5(CHANGED_MD5);

        XmlMetadata collection = new XmlMetadata();
        collection.setPath(COLLECTION);

        modifiedMetadataMap.put(COLLECTION, collection);
        modifiedMetadataMap.put(XML_FILE, metadata);

        return modifiedMetadataMap;
    }

    /**
     * Generate XML file from given template.
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

        while ((line = reader.readLine())!=null) {
            bufferedWriter.write(line);
            bufferedWriter.newLine();
        }

        reader.close();

        bufferedWriter.close();
    }

    /**
     * @param metadata
     * @return
     */
    public static String getXMLMetadataString(XmlMetadata metadata) {
        return String.format("%s %s %s", metadata.getFileName(), metadata.getPath(), metadata.getMd5());
    }


}

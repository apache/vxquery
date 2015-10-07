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
package org.apache.vxquery.xtest.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.metadata.VXQueryIOFileFilter;
import org.apache.vxquery.types.AnyType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.xmlparser.SAXContentHandler;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.apache.vxquery.xtest.util.tests.BufferedParsedCharacterStream;
import org.apache.vxquery.xtest.util.tests.BufferedReaderBufferedStream;
import org.apache.vxquery.xtest.util.tests.BufferedReaderStream;
import org.apache.vxquery.xtest.util.tests.BufferedStream;
import org.apache.vxquery.xtest.util.tests.IDiskTest;
import org.apache.vxquery.xtest.util.tests.ParsedBufferedByteStream;
import org.apache.vxquery.xtest.util.tests.ParsedBufferedCharacterStream;
import org.apache.vxquery.xtest.util.tests.ParsedByteStream;
import org.apache.vxquery.xtest.util.tests.ParsedCharacterStream;
import org.apache.vxquery.xtest.util.tests.ReaderBufferedStream;
import org.apache.vxquery.xtest.util.tests.ReaderStream;
import org.apache.vxquery.xtest.util.tests.Stream;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class DiskPerformance {
    XMLReader parser;
    SAXContentHandler handler;
    Collection<File> cTestFiles;
    Iterator<File> testFilesIt;

    public DiskPerformance() {
        Pair<XMLReader, SAXContentHandler> p = getNewParser();
        parser = p.first;
        handler = p.second;
        cTestFiles = null;
    }

    public Pair<XMLReader, SAXContentHandler> getNewParser() {
        XMLReader parser;
        SAXContentHandler handler;

        try {
            parser = XMLReaderFactory.createXMLReader();
            List<SequenceType> childSeq = new ArrayList<SequenceType>();
            NameTest nt = new NameTest(createUTF8String(""), createUTF8String("data"));
            childSeq.add(SequenceType.create(new ElementType(nt, AnyType.INSTANCE, false), Quantifier.QUANT_ONE));
            handler = new SAXContentHandler(false, new TreeNodeIdProvider((short) 0), null, childSeq);
            parser.setContentHandler(handler);
            parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);
            return new Pair<XMLReader, SAXContentHandler>(parser, handler);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setDirectory(String filename) {
        File directory = new File(filename);
        if (directory.isDirectory()) {
            cTestFiles = FileUtils.listFiles(directory, new VXQueryIOFileFilter(), TrueFileFilter.INSTANCE);
            if (cTestFiles.size() < 1) {
                System.err.println("No XML files found in given directory.");
                return;
            }
        }
        testFilesIt = cTestFiles.iterator();
    }

    public File getNextFile() {
        if (!testFilesIt.hasNext()) {
            testFilesIt = cTestFiles.iterator();
        }
        return testFilesIt.next();
    }

    private byte[] createUTF8String(String str) {
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        StringValueBuilder svb = new StringValueBuilder();
        try {
            svb.write(str, abvs.getDataOutput());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return Arrays.copyOf(abvs.getByteArray(), abvs.getLength());
    }

    public static void main(String... args) throws IOException {
        // First Argument (XML folder)
        if (args.length < 1) {
            System.err.println("Please provide a directory for the test XML documents.");
            return;
        }
        // Second argument (threads)
        int threads = 1;
        if (args.length > 1) {
            threads = Integer.parseInt(args[1]);
        }

        // Third argument (repeat)
        int repeat = 1;
        if (args.length > 2) {
            repeat = Integer.parseInt(args[2]);
        }

        // Fourth argument (buffer size)
        int bufferSize = -1;
        if (args.length > 3) {
            bufferSize = Integer.parseInt(args[3]);
        }

        DiskPerformance dp = new DiskPerformance();
        dp.setDirectory(args[0]);

        ArrayList<Class> tests = new ArrayList<Class>();
        // Parsed Character Streams
        tests.add(ParsedBufferedCharacterStream.class);
        //        tests.add(BufferedParsedCharacterStream.class);
        //        tests.add(ParsedCharacterStream.class);
        // Parsed Byte Streams
        //        tests.add(ParsedBufferedByteStream.class);
        //        tests.add(ParsedByteStream.class);
        // Character Streams
        //        tests.add(BufferedReaderBufferedStream.class);
        //        tests.add(BufferedReaderStream.class);
        //        tests.add(ReaderBufferedStream.class);
        //        tests.add(ReaderStream.class);
        // Byte Streams
        //        tests.add(BufferedStream.class);
        //        tests.add(Stream.class);

        System.out.println("------");
        System.out.println("Started Test Group: " + new Date());
        System.out.println("Thread: " + threads);
        System.out.println("Repeat: " + repeat);
        System.out.println("Buffer: " + bufferSize);
        System.out.println("------");

        for (Class<IDiskTest> testClass : tests) {
            for (int r = 0; r < repeat; ++r) {
                try {
                    if (threads > 1) {
                        runThreadTest(testClass, dp, threads, bufferSize);
                    } else {
                        IDiskTest test = testClass.newInstance();
                        test.setFile(dp.getNextFile());
                        test.setBufferSize(bufferSize);
                        test.setParser(dp.parser);
                        test.run();
                    }
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

            }
        }

    }

    static <T> void runThreadTest(Class<T> testType, DiskPerformance dp, int threads, int bufferSize)
            throws InstantiationException, IllegalAccessException {
        ExecutorService es = Executors.newCachedThreadPool();
        ArrayList<IDiskTest> threadTests = new ArrayList<IDiskTest>();
        for (int i = 0; i < threads; ++i) {
            threadTests.add((IDiskTest) testType.newInstance());
        }
        for (IDiskTest test : threadTests) {
            test.setFile(dp.getNextFile());
            test.setBufferSize(bufferSize);
            test.setParser(dp.getNewParser().first);
            es.execute((Runnable) test);
        }
        es.shutdown();
        try {
            es.awaitTermination(60, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //        System.out.println("Completed thread batch: " + new Date());
    }
}

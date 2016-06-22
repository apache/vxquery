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
package org.apache.vxquery.xtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.namespace.QName;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public abstract class AbstractTestCaseFactory {
    protected TestConfiguration tConfig;
    protected File catalog;
    protected String baseDirectory;
    protected TestCase tc;
    protected Pattern include;
    protected Pattern exclude;
    protected Set<String> previousTestResults;
    protected XTestOptions opts;
    protected String nextVariable;
    protected boolean expectedError;
    protected boolean outputFile;
    protected int currPathLen;
    protected int count;

    protected static final int TEST_NAME_INDEX = 0;
    protected static final int TEST_RESULT_INDEX = 1;
    protected static final List<String> PASSING_TESTS = Arrays.asList("EXPECTED_RESULT_GOT_SAME_RESULT",
            "EXPECTED_ERROR_GOT_SAME_ERROR");

    public AbstractTestCaseFactory(XTestOptions opts) {
        System.err.println("opts.catalog: " + opts.catalog);
        this.catalog = new File(opts.catalog);
        this.baseDirectory = this.catalog.getParent();
        tConfig = new TestConfiguration();
        tConfig.options = opts;
        this.opts = opts;
        if (opts.include != null) {
            this.include = Pattern.compile(opts.include);
        }
        if (opts.exclude != null) {
            this.exclude = Pattern.compile(opts.exclude);
        }
        if (opts.previousTestResults != null) {
            this.previousTestResults = getPreviousTests(opts.previousTestResults);
        } else {
            this.previousTestResults = null;
        }
        try {
            currPathLen = new File(".").getCanonicalPath().length();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> getPreviousTests(String previousTestResults) {
        Set<String> tests = new LinkedHashSet<String>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(previousTestResults));
            String line;
            String[] resultRow;
            while ((line = br.readLine()) != null) {
                resultRow = line.split(",");
                if (PASSING_TESTS.contains(resultRow[TEST_RESULT_INDEX].trim())) {
                    tests.add(resultRow[TEST_NAME_INDEX].trim());
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tests;
    }

    public int process() throws Exception {
        count = 0;
        XMLReader parser = XMLReaderFactory.createXMLReader();
        nextVariable = null;
        Handler handler = new Handler();
        parser.setContentHandler(handler);
        parser.setEntityResolver(new EntityResolver() {
            @Override
            public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
                URL url = new URL(systemId);
                return new InputSource(
                        baseDirectory + new File(url.getFile()).getCanonicalPath().substring(currPathLen));
            }
        });
        try {
            FileReader characterStream = new FileReader(catalog);
            parser.parse(new InputSource(characterStream));
            characterStream.close();
        } catch (SAXException e) {
            System.err.println("Unable to parse file: " + catalog.getAbsolutePath());
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            System.err.println("Test Catalog has not been found: " + catalog.getAbsolutePath());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("I: " + catalog.getAbsolutePath());
            e.printStackTrace();
        }
        return count;
    }

    protected boolean submitTestCase(TestCase tc) {
        boolean toSubmit = include == null || include.matcher(tc.getXQueryDisplayName()).find();
        toSubmit = toSubmit && (exclude == null || !exclude.matcher(tc.getXQueryDisplayName()).find());
        if (previousTestResults != null) {
            toSubmit = previousTestResults.contains(tc.getXQueryDisplayName());
        }
        return toSubmit;
    }

    protected abstract void submit(TestCase tc);

    protected class Handler implements ContentHandler {
        StringBuilder buffer = null;

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (buffer == null) {
                buffer = new StringBuilder();
            }
            buffer.append(ch, start, length);
        }

        private void flushChars() {
            if (buffer == null) {
                return;
            }
            String str = buffer.toString();
            buffer = null;
            if (nextVariable != null) {
                if (tConfig.sourceFileMap.get(str) == null) {
                    System.err.println(tc.getXQueryFile());
                    System.err.println(str);
                }
                tc.addExternalVariableBinding(new QName(nextVariable), tConfig.sourceFileMap.get(str));
            } else if (expectedError) {
                tc.setExpectedError(str);
            } else if (outputFile) {
                ExpectedResult er = new ExpectedResult(str);
                tc.addExpectedResult(er);
            }
        }

        @Override
        public void endDocument() throws SAXException {
            flushChars();
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            flushChars();
            if ("test-case".equals(localName)) {
                if (tc != null) {
                    submit(tc);
                    tc = null;
                }
            } else if ("input-file".equals(localName)) {
                nextVariable = null;
            } else if ("output-file".equals(localName)) {
                outputFile = false;
            } else if ("expected-error".equals(localName)) {
                expectedError = false;
            }
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {
            flushChars();
        }

        @Override
        public void setDocumentLocator(Locator locator) {
            flushChars();
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
            flushChars();
        }

        @Override
        public void startDocument() throws SAXException {
            flushChars();
        }

        @Override
        public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
            flushChars();
            try {
                if ("query".equals(localName)) {
                    if (tc != null) {
                        String tcName = atts.getValue("", "name");
                        tc.setName(tcName);
                    }
                } else if ("expected-error".equals(localName)) {
                    if (tc != null) {
                        expectedError = true;
                    }
                } else if ("input-file".equals(localName)) {
                    nextVariable = atts.getValue("", "variable");
                } else if ("output-file".equals(localName)) {
                    outputFile = true;
                } else if ("test-case".equals(localName)) {
                    tc = new TestCase(tConfig);
                    String folder = atts.getValue("", "FilePath");
                    tc.setFolder(folder);
                } else if ("source".equals(localName)) {
                    String id = atts.getValue("", "ID");
                    File srcFile = new File(tConfig.testRoot, atts.getValue("", "FileName"));
                    tConfig.sourceFileMap.put(id, srcFile);
                } else if ("test-suite".equals(localName)) {
                    tConfig.testRoot = new File(new File(baseDirectory).getCanonicalFile(),
                            atts.getValue("", "SourceOffsetPath"));
                    tConfig.xqueryQueryOffsetPath = new File(tConfig.testRoot,
                            atts.getValue("", "XQueryQueryOffsetPath"));
                    tConfig.resultOffsetPath = new File(tConfig.testRoot, atts.getValue("", "ResultOffsetPath"));
                    tConfig.xqueryFileExtension = atts.getValue("", "XQueryFileExtension");
                    tConfig.xqueryxFileExtension = atts.getValue("", "XQueryXFileExtension");
                }
            } catch (Exception e) {
                throw new SAXException(e);
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
        }
    }
}

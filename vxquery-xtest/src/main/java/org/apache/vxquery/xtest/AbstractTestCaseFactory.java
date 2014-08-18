package org.apache.vxquery.xtest;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
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
    public TestConfiguration tConfig;
    public File catalog;
    public String baseDirectory;
    public TestCase tc;
    public Pattern include;
    public Pattern exclude;
    public XTestOptions opts;
    public String nextVariable;
    public boolean expectedError;
    public boolean outputFile;
    public int currPathLen;
    public int count;

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
        try {
            currPathLen = new File(".").getCanonicalPath().length();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                return new InputSource(baseDirectory
                        + new File(url.getFile()).getCanonicalPath().substring(currPathLen));
            }
        });
        parser.parse(new InputSource(new FileReader(catalog)));
        return count;
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
                    tConfig.testRoot = new File(new File(baseDirectory).getCanonicalFile(), atts.getValue("",
                            "SourceOffsetPath"));
                    tConfig.xqueryQueryOffsetPath = new File(tConfig.testRoot, atts.getValue("",
                            "XQueryQueryOffsetPath"));
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
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
package org.apache.vxquery.runtime.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.vxquery.datamodel.NodeConstructingEventAcceptor;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class XMLParserUtils {
    public static XDMNode parseFile(RuntimeControlBlock rcb, File file) throws SystemException {
        try {
            InputSource isrc = new InputSource(new FileInputStream(file));
            return parseInputSource(rcb, isrc);
        } catch (FileNotFoundException e) {
            throw new SystemException(ErrorCode.FODC0002, e, file);
        }
    }

    public static XDMNode parseInputSource(RuntimeControlBlock rcb, InputSource in) throws SystemException {
        NodeConstructingEventAcceptor acceptor = rcb.getNodeFactory().createDocumentConstructor();
        acceptor.open();

        XMLReader parser;
        try {
            parser = XMLReaderFactory.createXMLReader();
            ParseHandler handler = new ParseHandler(acceptor);
            parser.setContentHandler(handler);
            parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);
            parser.parse(in);
            acceptor.close();
            return acceptor.getConstructedNode();
        } catch (Exception e) {
            throw new SystemException(ErrorCode.FODC0002, e, in.getSystemId());
        }
    }

    private static class ParseHandler implements ContentHandler, LexicalHandler {
        private NodeConstructingEventAcceptor acceptor;
        private StringBuilder buffer;
        private boolean pendingText;

        public ParseHandler(NodeConstructingEventAcceptor acceptor) {
            this.acceptor = acceptor;
            buffer = new StringBuilder();
            pendingText = false;
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            buffer.append(ch, start, length);
            pendingText = true;
        }

        @Override
        public void endDocument() throws SAXException {
            try {
                flushText();
                acceptor.endDocument();
            } catch (SystemException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            try {
                flushText();
                acceptor.endElement();
            } catch (SystemException e) {
                e.printStackTrace();
                throw new SAXException(e);
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
            try {
                flushText();
                acceptor.pi(target, data);
            } catch (SystemException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
        }

        @Override
        public void startDocument() throws SAXException {
            try {
                flushText();
                acceptor.startDocument();
            } catch (SystemException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }

        @Override
        public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
            try {
                flushText();
                int idx = name.indexOf(':');
                String prefix = idx < 0 ? "" : name.substring(0, idx);
                acceptor.startElement(uri, localName, prefix);
                final int nAttrs = atts.getLength();
                for (int i = 0; i < nAttrs; ++i) {
                    String aName = atts.getQName(i);
                    int aIdx = aName.indexOf(':');
                    String aPrefix = aIdx < 0 ? "" : aName.substring(0, aIdx);
                    String aLocalName = atts.getLocalName(i);
                    String aUri = atts.getURI(i);
                    String aValue = atts.getValue(i);
                    acceptor.attribute(uri, localName, prefix, aValue);
                }
            } catch (SystemException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
        }

        @Override
        public void comment(char[] ch, int start, int length) throws SAXException {
            try {
                flushText();
                acceptor.comment(String.valueOf(ch, start, length));
            } catch (SystemException e) {
                e.printStackTrace();
                throw new SAXException(e);
            }
        }

        private void flushText() throws SystemException {
            if (pendingText) {
                acceptor.text(buffer);
                buffer.delete(0, buffer.length());
                pendingText = false;
            }
        }

        @Override
        public void endCDATA() throws SAXException {
        }

        @Override
        public void endDTD() throws SAXException {
        }

        @Override
        public void endEntity(String name) throws SAXException {
        }

        @Override
        public void startCDATA() throws SAXException {
        }

        @Override
        public void startDTD(String name, String publicId, String systemId) throws SAXException {
        }

        @Override
        public void startEntity(String name) throws SAXException {
        }
    }
}
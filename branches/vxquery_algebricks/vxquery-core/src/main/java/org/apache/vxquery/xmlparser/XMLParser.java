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
package org.apache.vxquery.xmlparser;

import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class XMLParser {
    public static void parseInputSource(InputSource in, IEventAcceptor acceptor) throws SystemException {
        XMLReader parser;
        try {
            parser = XMLReaderFactory.createXMLReader();
            SAXContentHandler handler = new SAXContentHandler(acceptor);
            parser.setContentHandler(handler);
            parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);
            acceptor.open();
            parser.parse(in);
            acceptor.close();
        } catch (Exception e) {
            throw new SystemException(ErrorCode.FODC0002, e, in.getSystemId());
        }
    }
}
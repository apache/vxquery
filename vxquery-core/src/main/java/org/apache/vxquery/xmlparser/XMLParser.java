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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.vxquery.exceptions.VXQueryFileNotFoundException;
import org.apache.vxquery.exceptions.VXQueryParseException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class XMLParser {
    XMLReader parser;
    SAXContentHandler handler;

    public XMLParser(boolean attachTypes, ITreeNodeIdProvider idProvider) throws HyracksDataException {
        try {
            parser = XMLReaderFactory.createXMLReader();
            handler = new SAXContentHandler(attachTypes, idProvider);
            parser.setContentHandler(handler);
            parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);
        } catch (Exception e) {
            throw new HyracksDataException(e.toString());
        }
    }


    public void parseFile(File file, InputSource in, ArrayBackedValueStorage abvs) throws HyracksDataException {
        try {
            if (file.getName().toLowerCase().endsWith(".xml.gz")) {
                in.setCharacterStream(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            } else {
                in.setCharacterStream(new InputStreamReader(new FileInputStream(file)));
            }
            parser.parse(in);
            handler.write(abvs);
        } catch (FileNotFoundException e) {
            throw new VXQueryFileNotFoundException(e);
        } catch (SAXException e) {
            throw new VXQueryParseException(e);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.VXQueryFileNotFoundException;
import org.apache.vxquery.exceptions.VXQueryParseException;
import org.apache.vxquery.types.SequenceType;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class XMLParser implements IParser {
    final XMLReader parser;
    final SAXContentHandler handler;
    final InputSource in;
    final String nodeId;
    final int bufferSize;

    public XMLParser(boolean attachTypes, ITreeNodeIdProvider idProvider, String nodeId) throws HyracksDataException {
        this(attachTypes, idProvider, nodeId, null, null, null);
    }

    public XMLParser(boolean attachTypes, ITreeNodeIdProvider idProvider, String nodeId, IFrameFieldAppender appender,
            List<Integer> childSeq, StaticContext staticContext) throws HyracksDataException {
        bufferSize = Integer.parseInt(System.getProperty("vxquery.buffer_size", "-1"));
        this.nodeId = nodeId;
        try {
            parser = XMLReaderFactory.createXMLReader();
            if (appender == null) {
                handler = new SAXContentHandler(attachTypes, idProvider, false);
            } else {
                List<SequenceType> childSequenceTypes = new ArrayList<SequenceType>();
                for (int typeCode : childSeq) {
                    childSequenceTypes.add(staticContext.lookupSequenceType(typeCode));
                }
                handler = new SAXContentHandler(attachTypes, idProvider, appender, childSequenceTypes);
            }
            parser.setContentHandler(handler);
            parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);
            in = new InputSource();
        } catch (Exception e) {
            throw new HyracksDataException(e.toString());
        }
    }

    public int parse(Reader input, ArrayBackedValueStorage abvs) throws HyracksDataException {
        try {
            in.setCharacterStream(input);
            parser.parse(in);
            handler.writeDocument(abvs);
            input.close();
        } catch (Exception e) {
            HyracksDataException hde = new HyracksDataException(e, nodeId);
            throw hde;
        }
        return 0;
    }

    public void parseElements(File file, IFrameWriter writer, int tupleIndex) throws HyracksDataException {
        try {
            Reader input;
            if (bufferSize > 0) {
                input = new BufferedReader(new InputStreamReader(new FileInputStream(file)), bufferSize);
            } else {
                input = new InputStreamReader(new FileInputStream(file));
            }
            in.setCharacterStream(input);
            handler.setupElementWriter(writer, tupleIndex);
            parser.parse(in);
            input.close();
        } catch (FileNotFoundException e) {
            HyracksDataException hde = new VXQueryFileNotFoundException(e, file, nodeId);
            throw hde;
        } catch (SAXException e) {
            HyracksDataException hde = new VXQueryParseException(e, file, nodeId);
            throw hde;
        } catch (IOException e) {
            HyracksDataException hde = new HyracksDataException(e, nodeId);
            throw hde;
        }
    }

    public void parseHDFSElements(InputStream inputStream, IFrameWriter writer, FrameTupleAccessor fta, int tupleIndex)
            throws HyracksDataException {
        try {
            Reader input;
            if (bufferSize > 0) {
                input = new BufferedReader(new InputStreamReader(inputStream), bufferSize);
            } else {
                input = new InputStreamReader(inputStream);
            }
            in.setCharacterStream(input);
            handler.setupElementWriter(writer, tupleIndex);
            parser.parse(in);
            input.close();
        } catch (Exception e) {
            HyracksDataException hde = new HyracksDataException(e, nodeId);
            throw hde;
        }
    }

}

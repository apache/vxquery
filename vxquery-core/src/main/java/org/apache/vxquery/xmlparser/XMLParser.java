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
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.VXQueryFileNotFoundException;
import org.apache.vxquery.exceptions.VXQueryParseException;
import org.apache.vxquery.types.SequenceType;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class XMLParser {
    final XMLReader parser;
    final SAXContentHandler handler;
    final InputSource in;
    final String nodeId;
    final int buffer_size;

    public XMLParser(boolean attachTypes, ITreeNodeIdProvider idProvider, String nodeId) throws HyracksDataException {
        this(attachTypes, idProvider, nodeId, null, null, null, null);
    }

    public XMLParser(boolean attachTypes, ITreeNodeIdProvider idProvider, String nodeId, ByteBuffer frame,
            FrameTupleAppender appender, List<Integer> childSeq, StaticContext staticContext)
            throws HyracksDataException {
        buffer_size = Integer.parseInt(System.getProperty("vxquery.buffer_size"));
        this.nodeId = nodeId;
        try {
            parser = XMLReaderFactory.createXMLReader();
            
            System.out.println("XMLReader buffer:" +parser.getProperty("http://apache.org/xml/properties/input-buffer-size"));
            if (frame == null || appender == null) {
                handler = new SAXContentHandler(attachTypes, idProvider);
            } else {
                List<SequenceType> childSequenceTypes = new ArrayList<SequenceType>();
                for (int typeCode : childSeq) {
                    childSequenceTypes.add(staticContext.lookupSequenceType(typeCode));
                }
                handler = new SAXContentHandler(attachTypes, idProvider, frame, appender, childSequenceTypes);
            }
            parser.setContentHandler(handler);
            parser.setProperty("http://xml.org/sax/properties/lexical-handler", handler);
            in = new InputSource();
        } catch (Exception e) {
            throw new HyracksDataException(e.toString());
        }
    }

    public void parseDocument(File file, ArrayBackedValueStorage abvs) throws HyracksDataException {
        try {
            in.setCharacterStream(new BufferedReader(new InputStreamReader(new FileInputStream(file)), buffer_size));
            parser.parse(in);
            handler.writeDocument(abvs);
        } catch (FileNotFoundException e) {
            HyracksDataException hde = new VXQueryFileNotFoundException(e, file);
            hde.setNodeId(nodeId);
            throw hde;
        } catch (SAXException e) {
            HyracksDataException hde = new VXQueryParseException(e, file);
            hde.setNodeId(nodeId);
            throw hde;
        } catch (IOException e) {
            HyracksDataException hde = new HyracksDataException(e);
            hde.setNodeId(nodeId);
            throw hde;
        }
    }

    public void parseElements(File file, IFrameWriter writer, FrameTupleAccessor fta, int tupleIndex)
            throws HyracksDataException {
        try {
            in.setCharacterStream(new BufferedReader(new InputStreamReader(new FileInputStream(file)), buffer_size));
            handler.setupElementWriter(writer, fta, tupleIndex);
            parser.parse(in);
        } catch (FileNotFoundException e) {
            HyracksDataException hde = new VXQueryFileNotFoundException(e, file);
            hde.setNodeId(nodeId);
            throw hde;
        } catch (SAXException e) {
            HyracksDataException hde = new VXQueryParseException(e, file);
            hde.setNodeId(nodeId);
            throw hde;
        } catch (IOException e) {
            HyracksDataException hde = new HyracksDataException(e);
            hde.setNodeId(nodeId);
            throw hde;
        }
    }

}
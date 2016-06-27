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
package org.apache.vxquery.jsonparser;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

import com.google.gson.stream.JsonToken;

public class JSONParser {
    protected JsonParser parser;
    protected final PointablePool ppool = PointablePoolFactory.INSTANCE.createPointablePool();
    protected final ArrayBuilder ab;
    final JsonFactory factory;
    protected JsonGenerator generator;
    protected final ArrayBackedValueStorage result;

    public JSONParser() throws JsonParseException, IOException {
        factory = new JsonFactory();
        parser = null;
        ab = new ArrayBuilder();
        generator = null;
        result = new ArrayBackedValueStorage();
    }

    public void parseDocument(File file, ArrayBackedValueStorage mvs)
            throws NumberFormatException, JsonParseException, IOException {
        Reader input = new BufferedReader(new InputStreamReader(new FileInputStream(file)),
                Integer.parseInt(System.getProperty("vxquery.buffer_size", "-1")));
        parser = factory.createJsonParser(input);
        if (parser.getCurrentToken().equals(JsonToken.BEGIN_ARRAY)) {
            writeArrayDocument(mvs);
        }
        input.close();
        //        while (!parser.isClosed()) {
        //            if (parser.getCurrentToken().equals(JsonToken.BEGIN_ARRAY)
        //                    || parser.getCurrentToken().equals(JsonToken.BEGIN_OBJECT)) {
        //                mvs.reset();
        //                ab.reset(mvs);
        //                TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
        //                ab.addItem(tempTvp);
        //            }
        //        }
        //        ab.finish();
    }

    public void writeArrayDocument(ArrayBackedValueStorage mvs) throws IOException {
        DataOutput out = mvs.getDataOutput();
        out.write(ValueTag.ARRAY_TAG);
        out.write(result.getByteArray(), result.getStartOffset(), result.getLength());
    }
}

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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonParseException;
import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.JsonToken;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;

public class JSONParser {

    final JsonFactory factory;
    protected final ArrayBackedValueStorage atomic;
    protected final List<ArrayBuilder> abStack;
    protected final List<ArrayBackedValueStorage> abvsStack;

    public JSONParser() throws JsonParseException, IOException {
        factory = new JsonFactory();
        atomic = new ArrayBackedValueStorage();
        abStack = new ArrayList<ArrayBuilder>();
        abvsStack = new ArrayList<ArrayBackedValueStorage>();
    }

    public void parseDocument(File file, ArrayBackedValueStorage result)
            throws NumberFormatException, JsonParseException, IOException {
        DataOutput outResult = result.getDataOutput();
        ArrayBackedValueStorage array = null;
        boolean inarray = false;
        boolean nestedarray = false;
        int counter = -1;
        JsonParser parser = factory.createParser(file);
        while (!parser.isClosed()) {
            JsonToken token = parser.nextToken();
            if (token == JsonToken.START_ARRAY) {
                if (!nestedarray) {
                    array = new ArrayBackedValueStorage();
                }
                ArrayBuilder ab = new ArrayBuilder();
                counter++;
                inarray = true;
                abvsStack.add(array);
                abStack.add(ab);
                abvsStack.get(counter).reset();
                abStack.get(counter).reset(abvsStack.get(counter));
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                atomic.reset();
                DataOutput out = atomic.getDataOutput();
                out.write(ValueTag.XS_INTEGER_TAG);
                out.writeLong(parser.getLongValue());
                if (inarray) {
                    abStack.get(counter).addItem(atomic);
                }
            }
            if (token == JsonToken.VALUE_STRING) {
                atomic.reset();
                StringValueBuilder svb = new StringValueBuilder();
                DataOutput out = atomic.getDataOutput();
                out.write(ValueTag.XS_STRING_TAG);
                svb.write(parser.getText(), out);
                if (inarray) {
                    abStack.get(counter).addItem(atomic);
                }
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                atomic.reset();
                DataOutput out = atomic.getDataOutput();
                out.write(ValueTag.XS_DOUBLE_TAG);
                out.writeDouble(parser.getDoubleValue());
                if (inarray) {
                    abStack.get(counter).addItem(atomic);
                }
            }
            if (token == JsonToken.END_ARRAY) {
                abStack.get(counter).finish();
                if (counter > 0) {
                    abStack.get(counter - 1).addItem(abvsStack.get(counter));
                    nestedarray = true;
                }
                counter--;
            }
        }
        if (inarray) {
            outResult.write(abvsStack.get(0).getByteArray());
        } else {
            outResult.write(atomic.getByteArray());
        }
    }
}
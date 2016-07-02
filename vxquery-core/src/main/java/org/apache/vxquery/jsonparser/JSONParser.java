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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;

public class JSONParser {

    final JsonFactory factory;
    protected final ArrayBackedValueStorage atomic;
    protected final List<ArrayBuilder> abStack;
    protected final List<ObjectBuilder> obStack;
    protected final List<ArrayBackedValueStorage> abvsStack;
    protected final List<UTF8StringPointable> spStack;
    protected final List<String> itemStack;

    public JSONParser() throws JsonParseException, IOException {
        factory = new JsonFactory();
        atomic = new ArrayBackedValueStorage();
        abStack = new ArrayList<ArrayBuilder>();
        obStack = new ArrayList<ObjectBuilder>();
        abvsStack = new ArrayList<ArrayBackedValueStorage>();
        spStack = new ArrayList<UTF8StringPointable>();
        itemStack = new ArrayList<String>();
    }

    public void parseDocument(File file, ArrayBackedValueStorage result)
            throws NumberFormatException, JsonParseException, IOException {
        DataOutput outResult = result.getDataOutput();
        DataOutput out = atomic.getDataOutput();
        StringValueBuilder svb = new StringValueBuilder();
        String lastItem = null;
        String startItem = null;
        boolean nested = false;
        JsonParser parser = factory.createParser(file);
        while (!parser.isClosed()) {
            JsonToken token = parser.nextToken();
            if (token == JsonToken.START_ARRAY) {
                if (!nested) {
                    abvsStack.add(new ArrayBackedValueStorage());
                    abStack.add(new ArrayBuilder());
                }
                itemStack.add("array");
                lastItem = "array";
                abvsStack.get(abvsStack.size() - 1).reset();
                abStack.get(abStack.size() - 1).reset(abvsStack.get(abvsStack.size() - 1));
            }
            if (token == JsonToken.START_OBJECT) {
                if (!nested) {
                    abvsStack.add(new ArrayBackedValueStorage());
                    obStack.add(new ObjectBuilder());
                }
                itemStack.add("object");
                lastItem = "object";
                abvsStack.get(abvsStack.size() - 1).reset();
                obStack.get(obStack.size() - 1).reset(abvsStack.get(abvsStack.size() - 1));
            }
            if (token == JsonToken.FIELD_NAME) {
                UTF8StringPointable sp = new UTF8StringPointable();
                ArrayBackedValueStorage key = new ArrayBackedValueStorage();
                key.reset();
                spStack.add(sp);
                DataOutput outk = key.getDataOutput();
                svb.write(parser.getText(), outk);
                spStack.get(spStack.size() - 1).set(key);
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                atomic.reset();
                out.write(ValueTag.XS_INTEGER_TAG);
                out.writeLong(parser.getLongValue());
                if (lastItem == "array") {
                    abStack.get(abStack.size() - 1).addItem(atomic);
                } else if (lastItem == "object") {
                    obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1), atomic);
                }
            }
            if (token == JsonToken.VALUE_STRING) {
                atomic.reset();
                out.write(ValueTag.XS_STRING_TAG);
                svb.write(parser.getText(), out);
                if (lastItem == "array") {
                    abStack.get(abStack.size() - 1).addItem(atomic);
                } else if (lastItem == "object") {
                    obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1), atomic);
                }
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                atomic.reset();
                out.write(ValueTag.XS_DOUBLE_TAG);
                out.writeDouble(parser.getDoubleValue());
                if (lastItem == "array") {
                    abStack.get(abStack.size() - 1).addItem(atomic);
                } else if (lastItem == "object") {
                    obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1), atomic);
                }
            }
            if (token == JsonToken.END_ARRAY) {
                if (itemStack.size() == 1) {
                    abStack.get(abStack.size() - 1).finish();
                    abStack.remove(abStack.size() - 1);
                    itemStack.remove(itemStack.size() - 1);
                    nested = true;
                    lastItem = "array";
                } else if (itemStack.get(itemStack.size() - 2) == "array") {
                    abStack.get(abStack.size() - 1).finish();
                    abStack.remove(abStack.size() - 1);
                    abStack.get(abStack.size() - 1).addItem(abvsStack.get(abvsStack.size() - 1));
                    abvsStack.remove(abvsStack.size() - 1);
                    itemStack.remove(itemStack.size() - 1);
                    lastItem = "array";
                } else if (itemStack.get(itemStack.size() - 2) == "object") {
                    abStack.get(abStack.size() - 1).finish();
                    abStack.remove(abStack.size() - 1);
                    obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1),
                            abvsStack.get(abvsStack.size() - 1));
                    abvsStack.remove(abvsStack.size() - 1);
                    itemStack.remove(itemStack.size() - 1);
                    nested = true;
                    lastItem = "object";
                }

                startItem = "array";

            }
            if (token == JsonToken.END_OBJECT) {
                if (itemStack.size() == 1) {
                    obStack.get(obStack.size() - 1).finish();
                    obStack.remove(obStack.size() - 1);
                    spStack.remove(spStack.size() - 1);
                    itemStack.remove(itemStack.size() - 1);
                    nested = true;
                    lastItem = "object";
                } else if (itemStack.get(itemStack.size() - 2) == "object") {
                    obStack.get(obStack.size() - 1).finish();
                    obStack.remove(obStack.size() - 1);
                    spStack.remove(spStack.size() - 1);
                    obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1),
                            abvsStack.get(abvsStack.size() - 1));
                    abvsStack.remove(abvsStack.size() - 1);
                    itemStack.remove(itemStack.size() - 1);
                    lastItem = "object";
                } else if (itemStack.get(itemStack.size() - 2) == "array") {
                    obStack.get(obStack.size() - 1).finish();
                    obStack.remove(obStack.size() - 1);
                    spStack.remove(spStack.size() - 1);
                    abStack.get(abStack.size() - 1).addItem(abvsStack.get(abvsStack.size() - 1));
                    abvsStack.remove(abvsStack.size() - 1);
                    itemStack.remove(itemStack.size() - 1);
                    nested = true;
                    lastItem = "array";
                }
                startItem = "object";
            }
        }
        if (startItem == "array" || startItem == "object") {
            outResult.write(abvsStack.get(0).getByteArray());
        } else {
            outResult.write(atomic.getByteArray());
        }
    }
}
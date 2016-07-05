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
    protected final List<ArrayBackedValueStorage> keyStack;
    protected final List<UTF8StringPointable> spStack;

    enum itemType {
        ARRAY,
        OBJECT
    }

    protected final List<itemType> itemStack;

    public JSONParser() throws JsonParseException {
        factory = new JsonFactory();
        atomic = new ArrayBackedValueStorage();
        abStack = new ArrayList<ArrayBuilder>();
        obStack = new ArrayList<ObjectBuilder>();
        abvsStack = new ArrayList<ArrayBackedValueStorage>();
        keyStack = new ArrayList<ArrayBackedValueStorage>();
        spStack = new ArrayList<UTF8StringPointable>();
        itemStack = new ArrayList<itemType>();
    }

    public void parseDocument(File file, ArrayBackedValueStorage result)
            throws NumberFormatException, JsonParseException, IOException {
        DataOutput outResult = result.getDataOutput();
        DataOutput out = atomic.getDataOutput();
        StringValueBuilder svb = new StringValueBuilder();
        String startItem = null;
        boolean nested = false;
        JsonParser parser = factory.createParser(file);
        JsonToken token = parser.nextToken();
        itemType checkItem = null;
        while (token != null) {
            if (itemStack.size() > 1) {
                checkItem = itemStack.get(itemStack.size() - 2);
            }
            switch (token) {
                case START_ARRAY:
                    if (!nested) {
                        abvsStack.add(new ArrayBackedValueStorage());
                        abStack.add(new ArrayBuilder());
                    }
                    itemStack.add(itemType.ARRAY);
                    abvsStack.get(abvsStack.size() - 1).reset();
                    abStack.get(abStack.size() - 1).reset(abvsStack.get(abvsStack.size() - 1));
                    break;
                case START_OBJECT:
                    if (!nested) {
                        abvsStack.add(new ArrayBackedValueStorage());
                        obStack.add(new ObjectBuilder());
                    }
                    itemStack.add(itemType.OBJECT);
                    abvsStack.get(abvsStack.size() - 1).reset();
                    obStack.get(obStack.size() - 1).reset(abvsStack.get(abvsStack.size() - 1));
                    break;
                case FIELD_NAME:
                    if (!nested) {
                        keyStack.add(new ArrayBackedValueStorage());
                        spStack.add(new UTF8StringPointable());
                    }
                    keyStack.get(keyStack.size() - 1).reset();
                    DataOutput outk = keyStack.get(keyStack.size() - 1).getDataOutput();
                    svb.write(parser.getText(), outk);
                    spStack.get(spStack.size() - 1).set(keyStack.get(keyStack.size() - 1));
                    break;
                case VALUE_NUMBER_INT:
                    atomicValues(ValueTag.XS_INTEGER_TAG, parser, out, svb);
                    break;
                case VALUE_STRING:
                    atomicValues(ValueTag.XS_STRING_TAG, parser, out, svb);
                    break;
                case VALUE_NUMBER_FLOAT:
                    atomicValues(ValueTag.XS_DOUBLE_TAG, parser, out, svb);
                    break;
                case END_ARRAY:
                    abStack.get(abStack.size() - 1).finish();
                    abStack.remove(abStack.size() - 1);
                    if (itemStack.size() == 1) {
                        nested = true;
                    } else if (checkItem == itemType.ARRAY) {
                        abStack.get(abStack.size() - 1).addItem(abvsStack.get(abvsStack.size() - 1));
                        abvsStack.remove(abvsStack.size() - 1);
                    } else if (checkItem == itemType.OBJECT) {
                        obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1),
                                abvsStack.get(abvsStack.size() - 1));
                        abvsStack.remove(abvsStack.size() - 1);
                        nested = true;
                    }
                    itemStack.remove(itemStack.size() - 1);
                    startItem = "array";
                    break;
                case END_OBJECT:
                    obStack.get(obStack.size() - 1).finish();
                    obStack.remove(obStack.size() - 1);
                    spStack.remove(spStack.size() - 1);
                    if (itemStack.size() == 1) {
                        nested = true;
                    } else if (checkItem == itemType.OBJECT) {
                        obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1),
                                abvsStack.get(abvsStack.size() - 1));
                        abvsStack.remove(abvsStack.size() - 1);
                    } else if (checkItem == itemType.ARRAY) {
                        abStack.get(abStack.size() - 1).addItem(abvsStack.get(abvsStack.size() - 1));
                        abvsStack.remove(abvsStack.size() - 1);
                        nested = true;
                    }
                    itemStack.remove(itemStack.size() - 1);
                    startItem = "object";
                    break;
                default:
                    break;
            }
            token = parser.nextToken();
        }
        if (startItem == "array" || startItem == "object") {
            outResult.write(abvsStack.get(0).getByteArray());
        } else {
            outResult.write(atomic.getByteArray());
        }
    }

    public void atomicValues(int tag, JsonParser parser, DataOutput out, StringValueBuilder svb) throws IOException {
        atomic.reset();
        out.write(tag);
        if (tag == ValueTag.XS_DOUBLE_TAG) {
            out.writeDouble(parser.getDoubleValue());
        } else if (tag == ValueTag.XS_STRING_TAG) {
            svb.write(parser.getText(), out);
        } else if (tag == ValueTag.XS_INTEGER_TAG) {
            out.writeLong(parser.getLongValue());
        }
        if (itemStack.size() != 0) {
            if (itemStack.get(itemStack.size() - 1) == itemType.ARRAY) {
                abStack.get(abStack.size() - 1).addItem(atomic);
            } else if (itemStack.get(itemStack.size() - 1) == itemType.OBJECT) {
                obStack.get(obStack.size() - 1).addItem(spStack.get(spStack.size() - 1), atomic);
            }
        }
    }
}

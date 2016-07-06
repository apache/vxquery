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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonParseException;
import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.JsonToken;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.xmlparser.IParser;

public class JSONParser implements IParser {

    final JsonFactory factory;
    protected final ArrayBackedValueStorage atomic;
    protected final List<ArrayBuilder> abStack;
    protected final List<ObjectBuilder> obStack;
    protected final List<ArrayBackedValueStorage> abvsStack;
    protected final List<ArrayBackedValueStorage> keyStack;
    protected final List<UTF8StringPointable> spStack;
    protected final StringValueBuilder svb;
    protected final DataOutput out;
    protected itemType checkItem, startItem;
    protected int levelArray, levelObject;

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
        svb = new StringValueBuilder();
        abvsStack.add(atomic);
        out = abvsStack.get(abvsStack.size() - 1).getDataOutput();

    }

    public void parseDocument(File file, ArrayBackedValueStorage result) throws HyracksDataException {
        try {
            DataOutput outResult = result.getDataOutput();
            JsonParser parser = factory.createParser(file);
            JsonToken token = parser.nextToken();
            checkItem = null;
            startItem = null;
            levelArray = 0;
            levelObject = 0;
            while (token != null) {
                if (itemStack.size() > 1) {
                    checkItem = itemStack.get(itemStack.size() - 2);
                }
                switch (token) {
                    case START_ARRAY:
                        levelArray++;
                        if (levelArray > abStack.size()) {
                            abStack.add(new ArrayBuilder());
                        }
                        if (levelArray + levelObject > abvsStack.size() - 1) {
                            abvsStack.add(new ArrayBackedValueStorage());
                        }
                        itemStack.add(itemType.ARRAY);
                        abvsStack.get(levelArray + levelObject).reset();
                        abStack.get(levelArray - 1).reset(abvsStack.get(levelArray + levelObject));
                        break;
                    case START_OBJECT:
                        levelObject++;
                        if (levelObject > obStack.size()) {
                            obStack.add(new ObjectBuilder());
                        }
                        if (levelArray + levelObject > abvsStack.size() - 1) {
                            abvsStack.add(new ArrayBackedValueStorage());
                        }
                        itemStack.add(itemType.OBJECT);
                        abvsStack.get(levelArray + levelObject).reset();
                        obStack.get(levelObject - 1).reset(abvsStack.get(levelArray + levelObject));
                        break;
                    case FIELD_NAME:
                        if (levelObject > spStack.size()) {
                            keyStack.add(new ArrayBackedValueStorage());
                            spStack.add(new UTF8StringPointable());
                        }
                        keyStack.get(levelObject - 1).reset();
                        DataOutput outk = keyStack.get(levelObject - 1).getDataOutput();
                        svb.write(parser.getText(), outk);
                        spStack.get(levelObject - 1).set(keyStack.get(levelObject - 1));
                        break;
                    case VALUE_NUMBER_INT:
                        atomicValues(ValueTag.XS_INTEGER_TAG, parser, out, svb, levelArray, levelObject);
                        break;
                    case VALUE_STRING:
                        atomicValues(ValueTag.XS_STRING_TAG, parser, out, svb, levelArray, levelObject);
                        break;
                    case VALUE_NUMBER_FLOAT:
                        atomicValues(ValueTag.XS_DOUBLE_TAG, parser, out, svb, levelArray, levelObject);
                        break;
                    case END_ARRAY:
                        abStack.get(levelArray - 1).finish();
                        if (itemStack.size() > 1) {
                            if (checkItem == itemType.ARRAY) {
                                abStack.get(levelArray - 2).addItem(abvsStack.get(levelArray + levelObject));
                            } else if (checkItem == itemType.OBJECT) {
                                obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1),
                                        abvsStack.get(levelArray + levelObject));
                            }
                        }
                        itemStack.remove(itemStack.size() - 1);
                        startItem = itemType.ARRAY;
                        levelArray--;
                        break;
                    case END_OBJECT:
                        obStack.get(levelObject - 1).finish();
                        if (itemStack.size() > 1) {
                            if (checkItem == itemType.OBJECT) {
                                obStack.get(levelObject - 2).addItem(spStack.get(levelObject - 2),
                                        abvsStack.get(levelArray + levelObject));
                            } else if (checkItem == itemType.ARRAY) {
                                abStack.get(levelArray - 1).addItem(abvsStack.get(levelArray + levelObject));
                            }
                        }
                        itemStack.remove(itemStack.size() - 1);
                        startItem = itemType.OBJECT;
                        levelObject--;
                        break;
                    default:
                        break;
                }
                token = parser.nextToken();
            }
            if (startItem == itemType.ARRAY || startItem == itemType.OBJECT) {
                outResult.write(abvsStack.get(1).getByteArray());
            } else {
                //the atomic value is always set to be at the bottom of the arraybackedvaluestorage stack.
                outResult.write(abvsStack.get(0).getByteArray());
            }
        } catch (Exception e) {
            throw new HyracksDataException(e.toString());
        }
    }

    public void atomicValues(int tag, JsonParser parser, DataOutput out, StringValueBuilder svb, int levelArray,
            int levelObject) throws IOException {
        abvsStack.get(0).reset();
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
                abStack.get(levelArray - 1).addItem(abvsStack.get(0));
            } else if (itemStack.get(itemStack.size() - 1) == itemType.OBJECT) {
                obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1), abvsStack.get(0));
            }
        }
    }

    @Override
    public void parseHDFSDocument(InputStream in, ArrayBackedValueStorage abvs) throws HyracksDataException {

    }
}

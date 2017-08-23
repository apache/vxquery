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
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.JsonToken;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.xmlparser.IParser;

public class JSONParser implements IParser {
    private final JsonFactory factory;
    private final List<IPointable> valuePointables;
    protected final ArrayBackedValueStorage atomic;
    private TaggedValuePointable tvp;
    protected final List<ArrayBuilder> abStack;
    protected final List<ObjectBuilder> obStack;
    protected final List<ArrayBackedValueStorage> abvsStack;
    protected final List<ArrayBackedValueStorage> keyStack;
    protected final List<UTF8StringPointable> spStack;
    protected final StringValueBuilder svb;
    protected final SequenceBuilder sb;
    protected final DataOutput out;
    protected itemType checkItem;
    protected int levelArray;
    protected int levelObject;
    protected final List<Object> allKeys;
    protected boolean matched;
    protected ArrayBackedValueStorage tempABVS;
    protected List<Integer> arrayCounters;
    protected IFrameWriter writer;
    protected IFrameFieldAppender appender;
    protected boolean[] matchedKeys;
    protected Object[] subelements;
    protected boolean skipping;

    enum itemType {
        ARRAY,
        OBJECT
    }

    protected final List<itemType> itemStack;

    public JSONParser() {
        this(new ArrayList<>());
    }

    public JSONParser(List<IPointable> valuePointables) {
        factory = new JsonFactory();
        this.valuePointables = valuePointables;
        atomic = new ArrayBackedValueStorage();
        tvp = new TaggedValuePointable();
        abStack = new ArrayList<>();
        obStack = new ArrayList<>();
        abvsStack = new ArrayList<>();
        keyStack = new ArrayList<>();
        spStack = new ArrayList<>();
        itemStack = new ArrayList<>();
        svb = new StringValueBuilder();
        sb = new SequenceBuilder();
        allKeys = new ArrayList<>();
        abvsStack.add(atomic);
        out = abvsStack.get(abvsStack.size() - 1).getDataOutput();
        tempABVS = new ArrayBackedValueStorage();
        matched = false;
        arrayCounters = new ArrayList<>();
        subelements = new Object[valuePointables.size()];
        matchedKeys = new boolean[valuePointables.size()];
        skipping = true;
        for (int i = 0; i < valuePointables.size(); i++) {
            int start = valuePointables.get(i).getStartOffset() + 1;
            int length = valuePointables.get(i).getLength() - 1;
            if (((TaggedValuePointable) valuePointables.get(i)).getTag() == ValueTag.XS_INTEGER_TAG) {
                // access an item of an array
                subelements[i] = new Long(LongPointable.getLong(valuePointables.get(i).getByteArray(), start));
            } else if (((TaggedValuePointable) valuePointables.get(i)).getTag() == ValueTag.XS_BOOLEAN_TAG) {
                // access all the items of an array or all the keys of an object
                subelements[i] = new Boolean(BooleanPointable.getBoolean(valuePointables.get(i).getByteArray(), start));
            } else {
                UTF8StringPointable sp = new UTF8StringPointable();
                sp.set(valuePointables.get(i).getByteArray(), start, length);
                subelements[i] = sp.toString();
            }
        }
    }

    public int parse(Reader input, ArrayBackedValueStorage result, IFrameWriter writer, IFrameFieldAppender appender)
            throws HyracksDataException {
        this.writer = writer;
        this.appender = appender;
        if (!valuePointables.isEmpty()) {
            return parseElements(input, result);
        } else {
            return parse(input, result);
        }
    }

    public int parse(Reader input, ArrayBackedValueStorage result) throws HyracksDataException {
        int items = 0;
        try {
            DataOutput outResult = result.getDataOutput();
            JsonParser parser = factory.createParser(input);
            JsonToken token = parser.nextToken();
            checkItem = null;
            levelArray = 0;
            levelObject = 0;
            sb.reset(result);
            while (token != null) {
                if (itemStack.size() > 1) {
                    checkItem = itemStack.get(itemStack.size() - 2);
                }
                switch (token) {
                    case START_ARRAY:
                        startArray();
                        break;
                    case START_OBJECT:
                        startObject();
                        break;
                    case FIELD_NAME:
                        startFieldName(parser);
                        break;
                    case VALUE_NUMBER_INT:
                        startAtomicValues(ValueTag.XS_INTEGER_TAG, parser);
                        break;
                    case VALUE_STRING:
                        startAtomicValues(ValueTag.XS_STRING_TAG, parser);
                        break;
                    case VALUE_NUMBER_FLOAT:
                        startAtomicValues(ValueTag.XS_DOUBLE_TAG, parser);
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
                        levelArray--;
                        if (levelArray + levelObject == 0) {
                            sb.addItem(abvsStack.get(1));
                            items++;
                        }
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
                        levelObject--;
                        if (levelObject + levelArray == 0) {
                            sb.addItem(abvsStack.get(1));
                            items++;
                        }
                        break;
                    default:
                        break;
                }
                token = parser.nextToken();
            }
            sb.finish();
            outResult.write(result.getByteArray());
        } catch (Exception e) {
            throw new HyracksDataException("Accessing or writing in out of bounds space", e);
        }
        return items;
    }

    public int parseElements(Reader input, ArrayBackedValueStorage result) throws HyracksDataException {
        int items = 0;
        try {
            JsonParser parser = factory.createParser(input);
            JsonToken token = parser.nextToken();
            checkItem = null;

            this.matched = false;
            matchedKeys = new boolean[valuePointables.size()];
            levelArray = 0;
            levelObject = 0;
            sb.reset(result);
            while (token != null) {
                if (itemStack.size() > 1) {
                    checkItem = itemStack.get(itemStack.size() - 2);
                }
                switch (token) {
                    case START_ARRAY:
                        startArray();
                        break;
                    case START_OBJECT:
                        startObject();
                        break;
                    case FIELD_NAME:
                        startFieldName(parser);
                        break;
                    case VALUE_NUMBER_INT:
                        startAtomicValues(ValueTag.XS_INTEGER_TAG, parser);
                        break;
                    case VALUE_STRING:
                        startAtomicValues(ValueTag.XS_STRING_TAG, parser);
                        break;
                    case VALUE_NUMBER_FLOAT:
                        startAtomicValues(ValueTag.XS_DOUBLE_TAG, parser);
                        break;
                    case END_ARRAY:
                        //if the query doesn't ask for an atomic value
                        if (!matched && !skipping) {
                            abStack.get(levelArray - 1).finish();
                            if (itemStack.size() > 1) {
                                if (checkItem == itemType.ARRAY) {
                                    abStack.get(levelArray - 2).addItem(abvsStack.get(levelArray + levelObject));
                                    if (!matched) {
                                        items++;
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                        skipping = true;
                                    }
                                } else if (checkItem == itemType.OBJECT) {
                                    obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1),
                                            abvsStack.get(levelArray + levelObject));
                                    if (!matched) {
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                        skipping = true;
                                        items++;
                                    }
                                }
                            }
                        }
                        if (allKeys.size() - 1 >= 0) {
                            allKeys.remove(allKeys.size() - 1);
                        }
                        if (levelArray > 0) {
                            this.arrayCounters.remove(levelArray - 1);
                        }
                        itemStack.remove(itemStack.size() - 1);
                        if (levelArray > 0) {
                            levelArray--;
                        }
                        break;
                    case END_OBJECT:
                        //if the query doesn't ask for an atomic value
                        if (!matched && !skipping) {
                            //check if the path asked from the query includes the current path
                            obStack.get(levelObject - 1).finish();
                            if (itemStack.size() > 1) {
                                if (checkItem == itemType.OBJECT) {
                                    obStack.get(levelObject - 2).addItem(spStack.get(levelObject - 2),
                                            abvsStack.get(levelArray + levelObject));
                                    if (!this.matched) {
                                        items++;
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                        skipping = true;
                                    }
                                } else if (checkItem == itemType.ARRAY) {
                                    abStack.get(levelArray - 1).addItem(abvsStack.get(levelArray + levelObject));
                                    if (!matched) {
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                        skipping = true;
                                        items++;
                                    }
                                }
                            }
                        }
                        if (allKeys.size() - 1 >= 0) {
                            allKeys.remove(allKeys.size() - 1);
                        }
                        itemStack.remove(itemStack.size() - 1);
                        if (levelObject > 0) {
                            levelObject--;
                        }
                        break;
                    default:
                        break;
                }
                token = parser.nextToken();
            }
            sb.finish();
        } catch (Exception e) {
            throw new HyracksDataException("Accessing or writing in out of bounds space", e);
        }
        return items;
    }

    private boolean pathMatch() {
        boolean contains = true;
        if (!allKeys.isEmpty() && allKeys.size() <= valuePointables.size()) {
            if (allKeys.get(allKeys.size() - 1).equals(subelements[allKeys.size() - 1])) {
                matchedKeys[allKeys.size() - 1] = true;
            } else {
                matchedKeys[allKeys.size() - 1] = false;
            }
        }
        for (boolean b : matchedKeys) {
            if (!b) {
                contains = false;
            }
        }
        if (contains) {
            skipping = false;
        }
        return contains;
    }

    public void itemsInArray() {
        if (!itemStack.isEmpty() && itemStack.get(itemStack.size() - 1) == itemType.ARRAY
                && !this.arrayCounters.isEmpty()) {
            boolean addCounter = subelements[allKeys.size()].equals(Boolean.TRUE) ? false : true;
            if (addCounter) {
                this.arrayCounters.set(levelArray - 1, this.arrayCounters.get(levelArray - 1) + 1);
                this.allKeys.add(new Long(this.arrayCounters.get(levelArray - 1)));
            } else {
                this.allKeys.add(Boolean.TRUE);
            }
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
        if (!itemStack.isEmpty()) {
            if (itemStack.get(itemStack.size() - 1) == itemType.ARRAY) {
                if (levelArray > 0) {
                    abStack.get(levelArray - 1).addItem(abvsStack.get(0));
                }
                if (!valuePointables.isEmpty()
                        && allKeys.get(allKeys.size() - 1).equals(subelements[subelements.length - 1])) {
                    writeElement(abvsStack.get(0));
                    matched = true;
                    skipping = true;
                }
            } else if (itemStack.get(itemStack.size() - 1) == itemType.OBJECT) {
                if (levelObject > 0) {
                    obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1), abvsStack.get(0));
                }
                if (!valuePointables.isEmpty()
                        && allKeys.get(allKeys.size() - 1).equals(subelements[subelements.length - 1])) {
                    writeElement(abvsStack.get(0));
                    matched = true;
                    skipping = true;
                }
            }
        }
    }

    public void writeElement(ArrayBackedValueStorage abvs) throws IOException {
        tempABVS.reset();
        DataOutput dOut = tempABVS.getDataOutput();
        dOut.write(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
        FrameUtils.appendFieldToWriter(writer, appender, tempABVS.getByteArray(), tempABVS.getStartOffset(),
                tempABVS.getLength());
    }

    public void startArrayOrObjects(int count) {
        if (!valuePointables.isEmpty() && !this.arrayCounters.isEmpty() && levelArray > 0) {
            boolean addCounter = subelements[allKeys.size()].equals(Boolean.TRUE) ? false : true;
            if (itemStack.get(itemStack.size() - 1) == itemType.ARRAY) {
                if (addCounter) {
                    this.arrayCounters.set(levelArray - count, this.arrayCounters.get(levelArray - count) + 1);
                    this.allKeys.add(new Long(this.arrayCounters.get(levelArray - count)));
                } else {
                    this.allKeys.add(Boolean.TRUE);
                }
            }

        }
        if (count == 2 && !valuePointables.isEmpty()) {
            this.arrayCounters.add(Integer.valueOf(0));
        }
    }

    public void startArray() throws HyracksDataException {
        startArrayOrObjects(2);
        itemStack.add(itemType.ARRAY);
        if (this.pathMatch() || valuePointables.isEmpty() || subelements[allKeys.size()].equals(Boolean.TRUE)
                || subelements[allKeys.size()] instanceof Long) {
            levelArray++;
            if (levelArray > abStack.size()) {
                abStack.add(new ArrayBuilder());
            }
            if (levelArray + levelObject > abvsStack.size() - 1) {
                abvsStack.add(new ArrayBackedValueStorage());
            }
            abvsStack.get(levelArray + levelObject).reset();
            try {
                abStack.get(levelArray - 1).reset(abvsStack.get(levelArray + levelObject));
            } catch (Exception e) {
                throw new HyracksDataException("Accessing index out of bounds", e);
            }
        }
    }

    public void startObject() throws HyracksDataException {
        startArrayOrObjects(1);
        itemStack.add(itemType.OBJECT);
        if (this.pathMatch() || valuePointables.isEmpty()) {
            levelObject++;
            if (levelObject > obStack.size()) {
                obStack.add(new ObjectBuilder());
            }
            if (levelArray + levelObject > abvsStack.size() - 1) {
                abvsStack.add(new ArrayBackedValueStorage());
            }
            abvsStack.get(levelArray + levelObject).reset();
            try {
                obStack.get(levelObject - 1).reset(abvsStack.get(levelArray + levelObject));
            } catch (Exception e) {
                throw new HyracksDataException("Accessing index out of bounds", e);
            }
        }
    }

    public void startFieldName(JsonParser parser) throws HyracksDataException {
        if (levelObject > spStack.size() || spStack.isEmpty()) {
            keyStack.add(new ArrayBackedValueStorage());
            spStack.add(new UTF8StringPointable());
        }
        try {
            allKeys.add(parser.getText());
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        int keyaccess;
        if (valuePointables.isEmpty()) {
            keyaccess = levelObject;
        } else {
            keyaccess = keyStack.size();
        }
        keyStack.get(keyaccess - 1).reset();
        DataOutput outk = keyStack.get(keyaccess - 1).getDataOutput();
        try {
            svb.write(parser.getText(), outk);
            spStack.get(keyaccess - 1).set(keyStack.get(keyaccess - 1));
            if (!valuePointables.isEmpty()) {
                //if the next two bytes represent a boolean (boolean has only two bytes),
                //it means that query asks for all the keys of the object
                if (allKeys.size() == valuePointables.size()) {
                    if (allKeys.get(allKeys.size() - 2).equals(subelements[allKeys.size() - 2])) {
                        tvp.set(valuePointables.get(allKeys.size() - 1));
                        if (tvp.getTag() == ValueTag.XS_BOOLEAN_TAG) {
                            abvsStack.get(0).reset();
                            out.write(ValueTag.XS_STRING_TAG);
                            svb.write(parser.getText(), out);
                            writeElement(abvsStack.get(0));
                            matchedKeys[allKeys.size() - 2] = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException("Writing in out of bounds space", e);
        }
    }

    public void startAtomicValues(int tag, JsonParser parser) throws HyracksDataException {
        itemsInArray();
        if (this.pathMatch() || valuePointables.isEmpty()) {
            try {
                atomicValues(tag, parser, out, svb, levelArray, levelObject);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
        if (allKeys.size() - 1 >= 0) {
            allKeys.remove(allKeys.size() - 1);
        }
    }
}

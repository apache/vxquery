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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.JsonToken;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.xmlparser.IParser;

public class JSONParser implements IParser {
    final JsonFactory factory;
    final List<Byte[]> valueSeq;
    protected final ArrayBackedValueStorage atomic;
    private TaggedValuePointable tvp;
    private BooleanPointable bp;
    protected final List<ArrayBuilder> abStack;
    protected final List<ObjectBuilder> obStack;
    protected final List<ArrayBackedValueStorage> abvsStack;
    protected final List<ArrayBackedValueStorage> keyStack;
    protected final List<UTF8StringPointable> spStack;
    protected final StringValueBuilder svb;
    protected final SequenceBuilder sb;
    protected final DataOutput out;
    protected itemType checkItem;
    protected int levelArray, levelObject;
    protected final List<Byte[]> allKeys;
    protected ByteArrayOutputStream outputStream, prefixStream, pathStream;
    protected int objectMatchLevel;
    protected int arrayMatchLevel;
    protected boolean matched, literal;
    protected ArrayBackedValueStorage tempABVS;
    protected List<Integer> arrayCounters;
    protected List<Boolean> keysOrMembers;
    protected IFrameWriter writer;
    protected IFrameFieldAppender appender;

    enum itemType {
        ARRAY,
        OBJECT
    }

    protected final List<itemType> itemStack;

    public JSONParser() {
        this(null);
    }

    public JSONParser(List<Byte[]> valueSeq) {
        factory = new JsonFactory();
        this.valueSeq = valueSeq;
        atomic = new ArrayBackedValueStorage();
        tvp = new TaggedValuePointable();
        abStack = new ArrayList<ArrayBuilder>();
        obStack = new ArrayList<ObjectBuilder>();
        abvsStack = new ArrayList<ArrayBackedValueStorage>();
        keyStack = new ArrayList<ArrayBackedValueStorage>();
        spStack = new ArrayList<UTF8StringPointable>();
        itemStack = new ArrayList<itemType>();
        svb = new StringValueBuilder();
        sb = new SequenceBuilder();
        bp = new BooleanPointable();
        allKeys = new ArrayList<Byte[]>();
        abvsStack.add(atomic);
        out = abvsStack.get(abvsStack.size() - 1).getDataOutput();
        tempABVS = new ArrayBackedValueStorage();
        this.objectMatchLevel = 1;
        this.arrayMatchLevel = 0;
        matched = false;
        literal = false;
        arrayCounters = new ArrayList<Integer>();
        outputStream = new ByteArrayOutputStream();
        prefixStream = new ByteArrayOutputStream();
        pathStream = new ByteArrayOutputStream();
        this.keysOrMembers = new ArrayList<Boolean>();
        outputStream.reset();
        pathStream.reset();
        if (valueSeq != null) {
            for (int i = 0; i < this.valueSeq.size(); i++) {
                tvp.set(ArrayUtils.toPrimitive(valueSeq.get(i)), 0, ArrayUtils.toPrimitive(valueSeq.get(i)).length);
                //access an item of an array
                if (tvp.getTag() == ValueTag.XS_INTEGER_TAG) {
                    pathStream.write(tvp.getByteArray(), 0, tvp.getLength());
                    this.arrayMatchLevel++;
                    this.keysOrMembers.add(Boolean.valueOf(true));
                    //access all the items of an array or
                    //all the keys of an object
                } else if (tvp.getTag() == ValueTag.XS_BOOLEAN_TAG) {
                    pathStream.write(tvp.getByteArray(), 0, tvp.getLength());
                    this.arrayMatchLevel++;
                    this.keysOrMembers.add(Boolean.valueOf(false));
                    //access an object 
                } else {
                    pathStream.write(tvp.getByteArray(), 1, tvp.getLength() - 1);
                }
            }
        }
    }

    Byte[] toBytes(Integer v) {
        Byte[] barr = ArrayUtils.toObject(ByteBuffer.allocate(9).putLong(1, v).array());
        barr[0] = ValueTag.XS_INTEGER_TAG;
        return barr;
    }

    public int parse(Reader input, ArrayBackedValueStorage result, IFrameWriter writer, IFrameFieldAppender appender)
            throws HyracksDataException {
        this.writer = writer;
        this.appender = appender;
        if (this.valueSeq != null) {
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

            this.objectMatchLevel = 0;
            this.matched = false;

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
                        if (!this.literal && this.pathMatch()) {
                            //check if the path asked from the query includes the current path 
                            abStack.get(levelArray - 1).finish();
                            if (itemStack.size() > 1) {
                                if (checkItem == itemType.ARRAY) {
                                    if (levelArray > this.arrayMatchLevel + 1) {
                                        abStack.get(levelArray - 2).addItem(abvsStack.get(levelArray + levelObject));
                                    } else if (this.matched) {
                                        this.matched = false;
                                        items++;
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                    }
                                } else if (checkItem == itemType.OBJECT) {
                                    if (levelArray > this.arrayMatchLevel && !this.matched) {
                                        obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1),
                                                abvsStack.get(levelArray + levelObject));
                                    } else if (this.matched) {
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                        this.matched = false;
                                        items++;
                                    }
                                }
                            }
                        }
                        if (allKeys.size() - 1 >= 0) {
                            allKeys.remove(allKeys.size() - 1);
                        }
                        this.arrayCounters.remove(levelArray - 1);
                        itemStack.remove(itemStack.size() - 1);
                        levelArray--;
                        break;
                    case END_OBJECT:
                        //if the query doesn't ask for an atomic value
                        if (!this.literal && this.pathMatch()) {
                            //check if the path asked from the query includes the current path 
                            obStack.get(levelObject - 1).finish();
                            if (itemStack.size() > 1) {
                                if (checkItem == itemType.OBJECT) {
                                    if (levelObject > this.objectMatchLevel) {
                                        obStack.get(levelObject - 2).addItem(spStack.get(levelObject - 2),
                                                abvsStack.get(levelArray + levelObject));
                                    } else if (this.matched) {
                                        this.matched = false;
                                        items++;
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                    }
                                } else if (checkItem == itemType.ARRAY) {
                                    abStack.get(levelArray - 1).addItem(abvsStack.get(levelArray + levelObject));
                                    if (this.matched) {
                                        writeElement(abvsStack.get(levelArray + levelObject));
                                        this.matched = false;
                                    }
                                }
                            }
                        }
                        if (allKeys.size() - 1 >= 0) {
                            allKeys.remove(allKeys.size() - 1);
                        }
                        itemStack.remove(itemStack.size() - 1);
                        levelObject--;
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
        outputStream.reset();
        for (Byte[] bb : allKeys) {
            outputStream.write(ArrayUtils.toPrimitive(bb), 0, ArrayUtils.toPrimitive(bb).length);
        }
        //the path of values created by parsing the file 
        boolean contains = false;
        this.matched = false;
        prefixStream.reset();
        if (pathStream.size() < outputStream.size()) {
            prefixStream.write(outputStream.toByteArray(), 0, pathStream.size());
            contains = Arrays.equals(prefixStream.toByteArray(), pathStream.toByteArray());
        } else {
            prefixStream.write(pathStream.toByteArray(), 0, outputStream.size());
            contains = Arrays.equals(prefixStream.toByteArray(), outputStream.toByteArray());
        }
        if (pathStream.size() == outputStream.size() && contains) {
            this.objectMatchLevel = this.levelObject;
            this.matched = true;
            this.literal = false;
        }
        return contains;
    }

    public void itemsInArray() {
        if (itemStack.get(itemStack.size() - 1) == itemType.ARRAY && !this.arrayCounters.isEmpty()) {
            boolean addCounter = levelArray - 1 < this.keysOrMembers.size() ? this.keysOrMembers.get(levelArray - 1)
                    : true;
            if (addCounter) {
                this.arrayCounters.set(levelArray - 1, this.arrayCounters.get(levelArray - 1) + 1);
                this.allKeys.add(this.toBytes(this.arrayCounters.get(levelArray - 1)));
            } else {
                Byte[] bool = { (byte) 0x2B, 0x01 };
                this.allKeys.add(bool);
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
                abStack.get(levelArray - 1).addItem(abvsStack.get(0));
                if (valueSeq != null && this.matched && levelArray == this.arrayMatchLevel) {
                    this.literal = true;
                    this.matched = false;
                    writeElement(abvsStack.get(0));
                }
            } else if (itemStack.get(itemStack.size() - 1) == itemType.OBJECT) {
                obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1), abvsStack.get(0));
                if (valueSeq != null && this.matched && levelObject == this.objectMatchLevel) {
                    this.literal = true;
                    this.matched = false;
                    writeElement(abvsStack.get(0));
                }
            }
        }
    }

    public void writeElement(ArrayBackedValueStorage abvs) throws IOException {
        tempABVS.reset();
        DataOutput out = tempABVS.getDataOutput();
        out.write(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
        FrameUtils.appendFieldToWriter(writer, appender, tempABVS.getByteArray(), tempABVS.getStartOffset(),
                tempABVS.getLength());
    }

    public void startArrayOrObjects(int count) {
        if (valueSeq != null && !this.arrayCounters.isEmpty()) {
            boolean addCounter = levelArray - count < this.keysOrMembers.size()
                    ? this.keysOrMembers.get(levelArray - count) : true;
            if (itemStack.get(itemStack.size() - 1) == itemType.ARRAY) {
                if (addCounter) {
                    this.arrayCounters.set(levelArray - count, this.arrayCounters.get(levelArray - count) + 1);
                    this.allKeys.add(this.toBytes(this.arrayCounters.get(levelArray - count)));
                } else {
                    XDMConstants.setTrue(bp);
                    this.allKeys.add(ArrayUtils.toObject(bp.getByteArray()));
                }
            }

        }
        if (count == 2 && valueSeq != null) {
            this.arrayCounters.add(Integer.valueOf(0));
        }
    }

    public void startArray() throws HyracksDataException {
        levelArray++;
        if (levelArray > abStack.size()) {
            abStack.add(new ArrayBuilder());
        }
        if (levelArray + levelObject > abvsStack.size() - 1) {
            abvsStack.add(new ArrayBackedValueStorage());
        }
        startArrayOrObjects(2);
        itemStack.add(itemType.ARRAY);
        if (this.pathMatch() || this.valueSeq == null) {
            abvsStack.get(levelArray + levelObject).reset();
            try {
                abStack.get(levelArray - 1).reset(abvsStack.get(levelArray + levelObject));
            } catch (Exception e) {
                throw new HyracksDataException("Accessing index out of bounds", e);
            }
        }
    }

    public void startObject() throws HyracksDataException {
        levelObject++;
        if (levelObject > obStack.size()) {
            obStack.add(new ObjectBuilder());
        }
        if (levelArray + levelObject > abvsStack.size() - 1) {
            abvsStack.add(new ArrayBackedValueStorage());
        }
        startArrayOrObjects(1);
        itemStack.add(itemType.OBJECT);
        if (this.pathMatch() || this.valueSeq == null) {
            abvsStack.get(levelArray + levelObject).reset();
            try {
                obStack.get(levelObject - 1).reset(abvsStack.get(levelArray + levelObject));
            } catch (Exception e) {
                throw new HyracksDataException("Accessing index out of bounds", e);
            }
        }
    }

    public void startFieldName(JsonParser parser) throws HyracksDataException {
        if (levelObject > spStack.size()) {
            keyStack.add(new ArrayBackedValueStorage());
            spStack.add(new UTF8StringPointable());
        }
        keyStack.get(levelObject - 1).reset();
        DataOutput outk = keyStack.get(levelObject - 1).getDataOutput();
        try {
            svb.write(parser.getText(), outk);
            spStack.get(levelObject - 1).set(keyStack.get(levelObject - 1));
            if (this.valueSeq != null) {
                int length = 0;
                byte[] barr = spStack.get(levelObject - 1).getByteArray();
                outputStream.reset();
                outputStream.write(barr, 0, spStack.get(levelObject - 1).getLength());
                allKeys.add(ArrayUtils.toObject(outputStream.toByteArray()));
                for (int i = 0; i < allKeys.size() - 1; i++) {
                    tvp.set(ArrayUtils.toPrimitive(allKeys.get(i)), 0, ArrayUtils.toPrimitive(allKeys.get(i)).length);
                    length += ArrayUtils.toPrimitive(allKeys.get(i)).length;
                }
                //if the next two bytes represent a boolean (boolean has only two bytes), 
                //it means that query asks for all the keys of the object
                if (length <= pathStream.size() && (length + 2) <= pathStream.size()) {
                    tvp.set(pathStream.toByteArray(), length, length + 2);
                    if (tvp.getTag() == ValueTag.XS_BOOLEAN_TAG) {
                        abvsStack.get(0).reset();
                        out.write(ValueTag.XS_STRING_TAG);
                        svb.write(parser.getText(), out);
                        writeElement(abvsStack.get(0));
                    }
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException("Writing in out of bounds space", e);
        }
    }

    public void startAtomicValues(int tag, JsonParser parser) throws HyracksDataException {
        itemsInArray();
        if (this.pathMatch() || this.valueSeq == null) {
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

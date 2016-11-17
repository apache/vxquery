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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.JsonToken;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.builders.jsonitem.ObjectBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.xmlparser.IParser;

public class JSONParser implements IParser {

	final JsonFactory factory;
	final List<Byte[]> valueSeq;
	protected final ArrayBackedValueStorage atomic;
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
	protected List<Integer> nestedPaths;
	protected List<Byte[]> paths;
	protected ByteArrayOutputStream outputStream;
	protected Byte[] b = { (byte) 0xff, 0x00, 0x00, 0x00 };
	protected int exactMatchLevel;
	protected boolean matched;
	protected List<Integer> arrayCounters;

	enum itemType {
		ARRAY, OBJECT
	}

	protected final List<itemType> itemStack;

	public JSONParser() {
		this(null);

	}

	public JSONParser(List<Byte[]> valueSeq) {
		factory = new JsonFactory();
		int count = 0;
		this.valueSeq = valueSeq;
		atomic = new ArrayBackedValueStorage();
		abStack = new ArrayList<ArrayBuilder>();
		obStack = new ArrayList<ObjectBuilder>();
		abvsStack = new ArrayList<ArrayBackedValueStorage>();
		keyStack = new ArrayList<ArrayBackedValueStorage>();
		spStack = new ArrayList<UTF8StringPointable>();
		itemStack = new ArrayList<itemType>();
		svb = new StringValueBuilder();
		sb = new SequenceBuilder();
		allKeys = new ArrayList<Byte[]>();
		abvsStack.add(atomic);
		nestedPaths = new ArrayList<Integer>();
		out = abvsStack.get(abvsStack.size() - 1).getDataOutput();
		paths = new ArrayList<Byte[]>();
		exactMatchLevel = 1;
		matched = false;
		arrayCounters = new ArrayList<Integer>();

		outputStream = new ByteArrayOutputStream();
		int size = 0;
		int temp = -1;
		byte[] barr;
		for (int i = 0; i < this.valueSeq.size(); i++) {
			if (Arrays.equals(this.valueSeq.get(i), b)) {
				if (outputStream.size() != 0) {
					barr = Arrays.copyOfRange(outputStream.toByteArray(), 0, size);
					paths.add(ArrayUtils.toObject(barr));
				}
				try {
					size = 0;
					outputStream.close();
					outputStream = new ByteArrayOutputStream();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				try {
					size = size + valueSeq.get(i)[2] + 2;
					barr = ArrayUtils.toPrimitive(valueSeq.get(i));
					barr = Arrays.copyOfRange(barr, 1, barr[2] + 3);
					outputStream.write(barr);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		if (this.valueSeq.size() > 0) {
			barr = Arrays.copyOfRange(outputStream.toByteArray(), 0, size);
			paths.add(ArrayUtils.toObject(barr));
		}
		for (int i = 0; i < this.valueSeq.size(); i++) {
			if (!Arrays.equals(this.valueSeq.get(i), b)) {
				count++;
				nestedPaths.add(temp, count);
			} else {
				temp++;
			}
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
					this.arrayCounters.add(new Integer(0));
					levelArray++;
					if (levelArray > abStack.size()) {
						abStack.add(new ArrayBuilder());
					}
					if (levelArray + levelObject > abvsStack.size() - 1) {
						abvsStack.add(new ArrayBackedValueStorage());
					}
					itemStack.add(itemType.ARRAY);
					if (this.pathMatch()) {
						abvsStack.get(levelArray + levelObject).reset();
						abStack.get(levelArray - 1).reset(abvsStack.get(levelArray + levelObject));
					}
					break;
				case START_OBJECT:
//					if (this.arrayCounters.size() > 0) {
//						this.arrayCounters.set(levelArray - 1, this.arrayCounters.get(levelArray - 1) + 1);
//						// allKeys.add(this.arrayCounters.get(levelArray).t);
//						// //Add as Byte the counter value
//					}
					levelObject++;
					if (levelObject > obStack.size()) {
						obStack.add(new ObjectBuilder());
					}
					if (levelArray + levelObject > abvsStack.size() - 1) {
						abvsStack.add(new ArrayBackedValueStorage());
					}
					itemStack.add(itemType.OBJECT);
					if (this.pathMatch()) {
						abvsStack.get(levelArray + levelObject).reset();
						obStack.get(levelObject - 1).reset(abvsStack.get(levelArray + levelObject));
					}
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
					byte[] barr = spStack.get(levelObject - 1).getByteArray();
					barr = Arrays.copyOfRange(barr, 0, 2 + barr[1]);
					allKeys.add(ArrayUtils.toObject(barr));
					break;
				case VALUE_NUMBER_INT:
					if (this.pathMatch()) {
						atomicValues(ValueTag.XS_INTEGER_TAG, parser, out, svb, levelArray, levelObject);
					}
					if (allKeys.size() - 1 >= 0)
						allKeys.remove(allKeys.size() - 1);
					break;
				case VALUE_STRING:
					if (this.pathMatch()) {
						atomicValues(ValueTag.XS_STRING_TAG, parser, out, svb, levelArray, levelObject);
					}
					if (allKeys.size() - 1 >= 0)
						allKeys.remove(allKeys.size() - 1);
					break;
				case VALUE_NUMBER_FLOAT:
					if (this.pathMatch()) {
						atomicValues(ValueTag.XS_DOUBLE_TAG, parser, out, svb, levelArray, levelObject);
					}
					if (allKeys.size() - 1 >= 0)
						allKeys.remove(allKeys.size() - 1);
					break;
				case END_ARRAY:
					if (this.pathMatch()) {
						abStack.get(levelArray - 1).finish();
						if (itemStack.size() > 1) {
							if (checkItem == itemType.ARRAY) {
								abStack.get(levelArray - 2).addItem(abvsStack.get(levelArray + levelObject));
							} else if (checkItem == itemType.OBJECT) {
//								obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1),
//										abvsStack.get(levelArray + levelObject));
								if (levelArray > this.exactMatchLevel) {
									obStack.get(levelObject - 1).addItem(spStack.get(levelObject - 1),
											abvsStack.get(levelArray + levelObject));
								} else if (this.matched) {
									sb.addItem(abvsStack.get(levelArray + levelObject));
									this.matched = false;
								}
							}
						}
					}
					if (allKeys.size() - 1 >= 0) {
						allKeys.remove(allKeys.size() - 1);
					}

					itemStack.remove(itemStack.size() - 1);
					levelArray--;
					if (levelArray + levelObject == 0) {
						sb.addItem(abvsStack.get(1));
						items++;
					}
					break;
				case END_OBJECT:
					if (this.pathMatch()) {
						obStack.get(levelObject - 1).finish();
						if (itemStack.size() > 1) {
							if (checkItem == itemType.OBJECT) {
								if (levelObject > this.exactMatchLevel) {
									obStack.get(levelObject - 2).addItem(spStack.get(levelObject - 2),
											abvsStack.get(levelArray + levelObject));
								} else if (this.matched) {
									sb.addItem(abvsStack.get(levelArray + levelObject));
									this.matched = false;
								}
							} else if (checkItem == itemType.ARRAY) {
								abStack.get(levelArray - 1).addItem(abvsStack.get(levelArray + levelObject));
								allKeys.add(allKeys.get(allKeys.size()-1));
							}
						}
					}
					if (allKeys.size() - 1 >= 0) {
						allKeys.remove(allKeys.size() - 1);
					}
					itemStack.remove(itemStack.size() - 1);
					levelObject--;
					if (levelObject + levelArray == 0) {
						// sb.addItem(abvsStack.get(1));
						// sb.addItem(abvsStack.get(this.exactMatchLevel));
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
			throw new HyracksDataException(e.toString());
		}
		return items;
	}

	private boolean pathMatch() {
		Byte[] curr;
		int size = 0;

		try {
			outputStream.close();
			outputStream = new ByteArrayOutputStream();
			for (Byte[] bb : allKeys) {
				size = size + ArrayUtils.toPrimitive(bb)[1] + 2;
				outputStream.write(ArrayUtils.toPrimitive(bb));
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		curr = ArrayUtils.toObject(Arrays.copyOfRange(outputStream.toByteArray(), 0, size));
		boolean contains = false;
		Byte[] prefix;
		for (Byte[] path : paths) {
			if (path.length < curr.length) {
				prefix = Arrays.copyOfRange(curr, 0, path.length);
				contains = contains || Arrays.equals(prefix, path);
			} else {
				if (curr.length > 0) {
					prefix = Arrays.copyOfRange(path, 0, curr.length);
					contains = contains || Arrays.equals(prefix, curr);
				}
			}

			if (path.length == curr.length && contains) {
				this.exactMatchLevel = this.levelObject;
				this.matched = true;
			}
		}

		return contains;
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
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.datamodel.values;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.jsonItem.ObjectBuilder;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;

public class XDMConstants {
    private static final byte[] BOOLEAN_TRUE_CONSTANT;

    private static final byte[] BOOLEAN_FALSE_CONSTANT;

    private static final byte[] EMPTY_SEQUENCE;

    private static final byte[] EMPTY_OBJECT;

    private static final byte[] EMPTY_STRING;

    static {
        BOOLEAN_TRUE_CONSTANT = new byte[2];
        BOOLEAN_TRUE_CONSTANT[0] = ValueTag.XS_BOOLEAN_TAG;
        BooleanPointable.setBoolean(BOOLEAN_TRUE_CONSTANT, 1, true);

        BOOLEAN_FALSE_CONSTANT = new byte[2];
        BOOLEAN_FALSE_CONSTANT[0] = ValueTag.XS_BOOLEAN_TAG;
        BooleanPointable.setBoolean(BOOLEAN_FALSE_CONSTANT, 1, false);

        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        SequenceBuilder sb = new SequenceBuilder();
        sb.reset(abvs);
        try {
            sb.finish();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        EMPTY_SEQUENCE = Arrays.copyOf(abvs.getByteArray(), abvs.getLength());

        abvs.reset();
        ObjectBuilder ob = new ObjectBuilder();
        ob.reset(abvs);
        try {
            ob.finish();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        EMPTY_OBJECT = Arrays.copyOf(abvs.getByteArray(), abvs.getLength());

        EMPTY_STRING = new byte[3];
        EMPTY_STRING[0] = ValueTag.XS_STRING_TAG;
        EMPTY_STRING[1] = 0;
        EMPTY_STRING[2] = 0;
    }

    private XDMConstants() {
    }

    public static void setTrue(IPointable p) {
        set(p, BOOLEAN_TRUE_CONSTANT);
    }

    public static void setFalse(IPointable p) {
        set(p, BOOLEAN_FALSE_CONSTANT);
    }

    public static void setEmptySequence(IPointable p) {
        set(p, EMPTY_SEQUENCE);
    }

    public static void setEmptyObject(IPointable p) {
        set(p, EMPTY_OBJECT);
    }

    public static void setEmptyString(IPointable p) {
        set(p, EMPTY_STRING);
    }

    private static void set(IPointable p, byte[] array) {
        p.set(array, 0, array.length);
    }
}

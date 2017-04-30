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
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;

public class XDMConstants {
    private static final byte[] BOOLEAN_TRUE_CONSTANT;

    private static final byte[] BOOLEAN_FALSE_CONSTANT;

    private static final byte[] EMPTY_SEQUENCE;

    private static final byte[] EMPTY_STRING;

    private static final byte[] JS_NULL_CONSTANT;

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

        EMPTY_STRING = new byte[2];
        EMPTY_STRING[0] = ValueTag.XS_STRING_TAG;
        EMPTY_STRING[1] = 0;

        JS_NULL_CONSTANT = new byte[1];
        JS_NULL_CONSTANT[0] = ValueTag.JS_NULL_TAG;
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

    public static void setEmptyString(IPointable p) {
        set(p, EMPTY_STRING);
    }

    public static void setJsNull(IPointable p) {
        set(p, JS_NULL_CONSTANT);
    }

    private static void set(IPointable p, byte[] array) {
        p.set(array, 0, array.length);
    }
}

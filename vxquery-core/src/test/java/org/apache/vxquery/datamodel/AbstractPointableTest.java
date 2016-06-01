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
package org.apache.vxquery.datamodel;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;

public abstract class AbstractPointableTest {
    private final ArrayBackedValueStorage abvsInput = new ArrayBackedValueStorage();
    private final StringValueBuilder svb = new StringValueBuilder();
    private boolean includeTag = true;

    protected void getTaggedValuePointable(Object value, IPointable result) throws IOException {
        int start = abvsInput.getLength();
        if (value instanceof java.lang.Integer) {
            writeInteger((Integer) value, abvsInput.getDataOutput());
        } else if (value instanceof java.lang.Long) {
            writeLong((Long) value, abvsInput.getDataOutput());
        } else if (value instanceof java.lang.Double) {
            writeDouble((Double) value, abvsInput.getDataOutput());
        } else if (value instanceof java.lang.String) {
            writeString((String) value, abvsInput.getDataOutput());
        } else {
            throw new IOException("Unknown object type for tagged value pointable.");
        }
        result.set(abvsInput.getByteArray(), start, abvsInput.getLength() - start);
    }

    protected void getTaggedValuePointable(Object value, boolean includeTag, IPointable result) throws IOException {
        this.includeTag = includeTag;
        getTaggedValuePointable(value, result);
    }

    protected void writeInteger(Integer value, DataOutput dOut) throws IOException {
        if (includeTag) {
            dOut.write(ValueTag.XS_INT_TAG);
        }
        dOut.writeInt(value);
    }

    protected void writeLong(Long value, DataOutput dOut) throws IOException {
        if (includeTag) {
            dOut.write(ValueTag.XS_LONG_TAG);
        }
        dOut.writeLong(value);
    }

    protected void writeDouble(Double value, DataOutput dOut) throws IOException {
        if (includeTag) {
            dOut.write(ValueTag.XS_DOUBLE_TAG);
        }
        dOut.writeDouble(value);
    }

    protected void writeString(String value, DataOutput dOut) throws IOException {
        if (includeTag) {
            dOut.write(ValueTag.XS_STRING_TAG);
        }
        svb.write(value, dOut);
    }

}

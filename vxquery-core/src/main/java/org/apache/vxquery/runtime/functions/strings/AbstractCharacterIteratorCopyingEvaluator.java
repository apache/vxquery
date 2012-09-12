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
package org.apache.vxquery.runtime.functions.strings;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractCharacterIteratorCopyingEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    private final ICharacterIterator charIterator;
    private final ArrayBackedValueStorage abvs;

    public AbstractCharacterIteratorCopyingEvaluator(IScalarEvaluator[] args, ICharacterIterator charIterator) {
        super(args);
        this.charIterator = charIterator;
        abvs = new ArrayBackedValueStorage();
    }

    @Override
    public final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        preEvaluate(args);
        abvs.reset();
        charIterator.reset();
        try {
            // Byte Format: Type (1 byte) + String Length (2 bytes) + String.
            DataOutput out = abvs.getDataOutput();
            out.write(ValueTag.XS_STRING_TAG);

            // Default values for the length and update later
            out.write(0xFF);
            out.write(0xFF);

            int c;
            while (ICharacterIterator.EOS_CHAR != (c = charIterator.next())) {
                if ((c >= 0x0001) && (c <= 0x007F)) {
                    out.write((byte) c);
                } else if (c > 0x07FF) {
                    out.write((byte) (0xE0 | ((c >> 12) & 0x0F)));
                    out.write((byte) (0x80 | ((c >> 6) & 0x3F)));
                    out.write((byte) (0x80 | ((c >> 0) & 0x3F)));
                } else {
                    out.write((byte) (0xC0 | ((c >> 6) & 0x1F)));
                    out.write((byte) (0x80 | ((c >> 0) & 0x3F)));
                }
            }

            // Update the full length string in the byte array.
            byte[] stringResult = abvs.getByteArray();
            stringResult[1] = (byte) (((abvs.getLength() - 3) >>> 8) & 0xFF);
            stringResult[2] = (byte) (((abvs.getLength() - 3) >>> 0) & 0xFF);

            result.set(stringResult, 0, abvs.getLength());
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    protected abstract void preEvaluate(TaggedValuePointable[] args) throws SystemException;

}

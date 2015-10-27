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
package org.apache.vxquery.runtime.functions.cast;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class CastToQNameOperation extends AbstractCastToOperation {
    ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
    DataOutput dOutInner = abvsInner.getDataOutput();

    @Override
    public void convertQName(XSQNamePointable qnamep, DataOutput dOut) throws SystemException, IOException {
        dOut.write(ValueTag.XS_QNAME_TAG);
        dOut.write(qnamep.getByteArray(), qnamep.getStartOffset(), qnamep.getLength());
    }

    @Override
    public void convertString(UTF8StringPointable stringp, DataOutput dOut) throws SystemException, IOException {
        abvsInner.reset();
        ICharacterIterator charIterator = new UTF8StringCharacterIterator(stringp);
        charIterator.reset();
        int c = 0;
        int prefixLength = 0;
        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (c == Character.valueOf(':')) {
                prefixLength = abvsInner.getLength();
            } else {
                FunctionHelper.writeChar((char) c, dOutInner);
            }
        }

        dOut.write(ValueTag.XS_QNAME_TAG);
        dOut.write((byte) ((0 >>> 8) & 0xFF));
        dOut.write((byte) ((0 >>> 0) & 0xFF));
        // No URI

        dOut.write((byte) ((prefixLength >>> 8) & 0xFF));
        dOut.write((byte) ((prefixLength >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset(), prefixLength);

        int localNameLength = abvsInner.getLength() - prefixLength;
        dOut.write((byte) ((localNameLength >>> 8) & 0xFF));
        dOut.write((byte) ((localNameLength >>> 0) & 0xFF));
        dOut.write(abvsInner.getByteArray(), abvsInner.getStartOffset() + prefixLength, localNameLength);
    }

}

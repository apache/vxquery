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

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class CastToQNameOperation extends AbstractCastToOperation {
    private static final int STRING_EXPECTED_LENGTH = 300;
    private final GrowableArray ga = new GrowableArray();
    private final UTF8StringBuilder sb = new UTF8StringBuilder();

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
        int c;
        boolean prefixFound = false;

        dOut.write(ValueTag.XS_QNAME_TAG);

        // URI
        dOut.write((byte) 0);

        // Prefix and Local Name
        ga.reset();
        sb.reset(ga, STRING_EXPECTED_LENGTH);
        while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
            if (c == Character.valueOf(':')) {
                prefixFound = true;
                break;
            } else {
                FunctionHelper.writeChar((char) c, sb);
            }
        }
        if (prefixFound) {
            // Finish Prefix
            sb.finish();
            dOut.write(ga.getByteArray(), 0, ga.getLength());

            // Local Name
            ga.reset();
            sb.reset(ga, STRING_EXPECTED_LENGTH);
            while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
                FunctionHelper.writeChar((char) c, sb);
            }
        } else {
            // No Prefix
            dOut.write((byte) 0);
            // Local Name is in ga variable
        }
        sb.finish();
        dOut.write(ga.getByteArray(), 0, ga.getLength());
    }

}

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
package org.apache.vxquery.datamodel.builders.atomic;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;

import org.apache.vxquery.datamodel.builders.base.AbstractBuilder;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;

public class UTF8StringBuilder extends AbstractBuilder {
    private IMutableValueStorage mvs;
    private DataOutput out;

    @Override
    public void reset(IMutableValueStorage mvs) throws IOException {
        this.mvs = mvs;
        out = mvs.getDataOutput();
        out.write(0);
        out.write(0);
    }

    @Override
    public void finish() throws IOException {
        int utflen = mvs.getLength() - 2;
        BytePointable.setByte(mvs.getByteArray(), 0, (byte) ((utflen >>> 8) & 0xFF));
        BytePointable.setByte(mvs.getByteArray(), 1, (byte) ((utflen >>> 0) & 0xFF));
    }

    public void appendCharArray(char[] ch, int start, int length) throws IOException {
        FunctionHelper.writeCharArray(ch, start, length, out);
        if (mvs.getLength() > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + mvs.getLength() + " bytes");
        }
    }
}
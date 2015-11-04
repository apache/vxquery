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
package org.apache.vxquery.result;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultUtils {
    public static String getStringFromBuffer(ByteBuffer buffer, IFrameTupleAccessor fta) throws HyracksDataException {
        String resultRecords = "";
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        try {
            fta.reset(buffer);
            for (int tIndex = 0; tIndex < fta.getTupleCount(); tIndex++) {
                int start = fta.getTupleStartOffset(tIndex);
                int length = fta.getTupleEndOffset(tIndex) - start;
                bbis.setByteBuffer(buffer, start);
                byte[] recordBytes = new byte[length];
                bbis.read(recordBytes, 0, length);
                resultRecords += new String(recordBytes, 0, length);
            }
        } finally {
            try {
                bbis.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        return resultRecords;
    }
}

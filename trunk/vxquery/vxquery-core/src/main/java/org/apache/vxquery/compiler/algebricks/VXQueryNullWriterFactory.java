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
package org.apache.vxquery.compiler.algebricks;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.XDMConstants;

import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class VXQueryNullWriterFactory implements INullWriterFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public INullWriter createNullWriter() {
        final VoidPointable vp = (VoidPointable) VoidPointable.FACTORY.createPointable();
        return new INullWriter() {
            @Override
            public void writeNull(DataOutput out) throws HyracksDataException {
                XDMConstants.setEmptySequence(vp);
                try {
                    out.write(vp.getByteArray(), vp.getStartOffset(), vp.getLength());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
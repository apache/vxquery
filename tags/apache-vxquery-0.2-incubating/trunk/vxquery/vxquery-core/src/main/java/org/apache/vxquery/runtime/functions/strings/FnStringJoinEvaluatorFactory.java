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

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnStringJoinEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnStringJoinEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final TaggedValuePointable tvp = new TaggedValuePointable();
        final UTF8StringPointable stringp1 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp2 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequencePointable seq = new SequencePointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                abvs.reset();

                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                // Only accept a sequence of strings and a string.
                if (!FunctionHelper.isDerivedFromString(tvp2.getTag())) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                if (FunctionHelper.isDerivedFromString(tvp1.getTag())) {
                    try {
                        // Return first parameter as a string.
                        DataOutput out = abvs.getDataOutput();
                        tvp1.getValue(stringp1);
                        out.write(ValueTag.XS_STRING_TAG);
                        out.write(stringp1.getByteArray(), stringp1.getStartOffset(), stringp1.getLength());
                        result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                    } catch (IOException e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                    return;
                } else if (tvp1.getTag() != ValueTag.SEQUENCE_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }

                // Operate on a sequence.
                tvp1.getValue(seq);
                tvp2.getValue(stringp2);

                try {
                    // Byte Format: Type (1 byte) + String Length (2 bytes) + String.
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.XS_STRING_TAG);

                    // Default values for the length and update later
                    out.write(0);
                    out.write(0);

                    int seqLen = seq.getEntryCount();
                    if (seqLen != 0) {
                        for (int j = 0; j < seqLen; ++j) {
                            // Add separator if more than one value.
                            if (j > 0) {
                                out.write(stringp2.getByteArray(), stringp2.getStartOffset() + 2,
                                        stringp2.getUTFLength());
                            }
                            // Get string from sequence.
                            seq.getEntry(j, tvp);
                            if (!FunctionHelper.isDerivedFromString(tvp.getTag())) {
                                throw new SystemException(ErrorCode.FORG0006);
                            }
                            tvp.getValue(stringp1);
                            out.write(stringp1.getByteArray(), stringp1.getStartOffset() + 2, stringp1.getUTFLength());
                        }

                        // Update the full length string in the byte array.
                        abvs.getByteArray()[1] = (byte) (((abvs.getLength() - 3) >>> 8) & 0xFF);
                        abvs.getByteArray()[2] = (byte) (((abvs.getLength() - 3) >>> 0) & 0xFF);
                    }

                    result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}

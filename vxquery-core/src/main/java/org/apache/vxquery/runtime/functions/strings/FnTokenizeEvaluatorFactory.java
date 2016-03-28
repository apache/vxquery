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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.util.GrowableIntArray;

import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class FnTokenizeEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnTokenizeEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp1 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp2 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp3 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final StringBuilder builder1 = new StringBuilder();
        final StringBuilder builder2 = new StringBuilder();
        final StringBuilder builder3 = new StringBuilder();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final GrowableIntArray slots = new GrowableIntArray();
        final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            private Pattern pattern = null;

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    abvs.reset();
                    slots.clear();
                    dataArea.reset();

                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];

                    // Only accept strings as input.
                    PatternMatchingEvaluatorUtils.checkInput(tvp, tvp1, seqp, stringp1);
                    stringp1.toString(builder1);
                    PatternMatchingEvaluatorUtils.checkInput(tvp, tvp2, seqp, stringp2);
                    stringp2.toString(builder2);
                    String s2 = builder2.toString();
                    // Third parameter is optional.
                    if (args.length > 2) {
                        TaggedValuePointable tvp3 = args[2];
                        if (!FunctionHelper.isDerivedFromString(tvp3.getTag())) {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                        tvp3.getValue(stringp3);
                        stringp3.toString(builder3);
                        String s3 = builder3.toString();
                        try {
                            if (s3.contains("q")) {
                                s2 = Pattern.quote(s2);
                            }
                            pattern = Pattern.compile(s2, PatternMatchingEvaluatorUtils.toFlag(s3));
                        } catch (PatternSyntaxException e) {
                            throw new SystemException(ErrorCode.FORX0002);
                        } catch (IllegalArgumentException e) {
                            throw new SystemException(ErrorCode.FORX0002);
                        }
                    }
                    if (pattern == null) {
                        try {
                            pattern = Pattern.compile(builder2.toString());
                        } catch (PatternSyntaxException e) {
                            throw new SystemException(ErrorCode.FORX0002);
                        }
                    }
                    try {
                        String[] match = pattern.split(builder1);
                        int l = match.length;
                        DataOutput output = dataArea.getDataOutput();
                        byte[] array;
                        int length;
                        for (int i = 0; i < l; ++i) {
                            length = match[i].length();
                            array = new byte[length + 3];
                            array[0] = ValueTag.XS_STRING_TAG;
                            array[1] = 0;
                            array[2] = (byte) match[i].length();
                            System.arraycopy(match[i].getBytes(), 0, array, 3, length);
                            output.write(array, 0, length + 3);
                            slots.append(dataArea.getLength());
                        }

                    } catch (IndexOutOfBoundsException e) {
                        throw new SystemException(ErrorCode.FORX0003);
                    }
                    finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            public void finish() throws IOException {
                DataOutput out = abvs.getDataOutput();
                if (slots.getSize() != 1) {
                    out.write(ValueTag.SEQUENCE_TAG);
                    int size = slots.getSize();
                    out.writeInt(size);
                    if (size > 0) {
                        int[] slotArray = slots.getArray();
                        for (int i = 0; i < size; ++i) {
                            out.writeInt(slotArray[i]);
                        }
                        out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
                    }
                } else {
                    out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
                }
            }
        };
    }
}

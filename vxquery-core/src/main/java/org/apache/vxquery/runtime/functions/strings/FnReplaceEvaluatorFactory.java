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
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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

public class FnReplaceEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnReplaceEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp1 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp2 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp3 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp4 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final StringBuilder builder1 = new StringBuilder();
        final StringBuilder builder2 = new StringBuilder();
        final StringBuilder builder3 = new StringBuilder();
        final StringBuilder builder4 = new StringBuilder();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            private Pattern pattern = null;
            private Matcher matcher = null;

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    // Byte Format: Type (1 byte) + String Length (2 bytes) + String.
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.XS_STRING_TAG);

                    // Default values for the length and update later
                    out.write(0);
                    out.write(0);

                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];
                    TaggedValuePointable tvp3 = args[2];

                    PatternMatchingEvaluatorUtils.checkInput(tvp, tvp1, seqp, stringp1);
                    stringp1.toString(builder1);
                    PatternMatchingEvaluatorUtils.checkInput(tvp, tvp2, seqp, stringp2);
                    stringp2.toString(builder2);
                    PatternMatchingEvaluatorUtils.checkInput(tvp, tvp3, seqp, stringp3);
                    stringp3.toString(builder3);

                    // Fourth parameter is optional.
                    if (args.length > 3) {
                        TaggedValuePointable tvp4 = args[3];
                        if (!FunctionHelper.isDerivedFromString(tvp4.getTag())) {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                        tvp4.getValue(stringp4);
                        stringp4.toString(builder4);
                        try {
                            pattern = Pattern.compile(builder2.toString(),
                                    PatternMatchingEvaluatorUtils.toFlag(builder4.toString()));
                        } catch (PatternSyntaxException e) {
                            throw new SystemException(ErrorCode.FORX0002);
                        } catch (IllegalArgumentException e) {
                            throw new SystemException(ErrorCode.FORX0001);
                        }
                    }
                    if (pattern == null) {
                        try {
                            pattern = Pattern.compile(builder2.toString());
                        } catch (PatternSyntaxException e) {
                            throw new SystemException(ErrorCode.FORX0002);
                        }
                    }

                    matcher = pattern.matcher(builder1.toString());
                    try {
                        String replaced = matcher.replaceAll(builder3.toString());
                        out.write(replaced.getBytes(StandardCharsets.UTF_8));
                    } catch (IndexOutOfBoundsException e) {
                        throw new SystemException(ErrorCode.FORX0003);
                    } catch (IllegalArgumentException e) {
                        throw new SystemException(ErrorCode.FORX0004);
                    }

                    abvs.getByteArray()[1] = (byte) (((abvs.getLength() - 3) >>> 8) & 0xFF);
                    abvs.getByteArray()[2] = (byte) (((abvs.getLength() - 3) >>> 0) & 0xFF);

                    result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}

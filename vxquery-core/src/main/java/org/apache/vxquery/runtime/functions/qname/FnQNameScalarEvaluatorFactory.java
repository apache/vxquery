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
package org.apache.vxquery.runtime.functions.qname;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.strings.ICharacterIterator;
import org.apache.vxquery.runtime.functions.strings.UTF8StringCharacterIterator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnQNameScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnQNameScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable paramURI = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable paramQName = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final ArrayBackedValueStorage abvsParamQName = new ArrayBackedValueStorage();
        final DataOutput dOutParamQName = abvsParamQName.getDataOutput();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                // Only accept a strings.
                if (args.length == 2) {
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seqp);
                        if (seqp.getEntryCount() == 0) {
                            XDMConstants.setEmptyString(tvp);
                            tvp.getValue(paramURI);
                        } else {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                    } else {
                        if (!FunctionHelper.isDerivedFromString(tvp1.getTag())) {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                        tvp1.getValue(paramURI);
                    }
                    if (tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp2.getValue(paramQName);
                } else if (args.length == 1) {
                    if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    XDMConstants.setEmptyString(tvp);
                    tvp.getValue(paramURI);
                    tvp2.getValue(paramQName);
                } else {
                    throw new SystemException(ErrorCode.FORG0006);
                }

                try {
                    abvs.reset();
                    dOut.write(ValueTag.XS_QNAME_TAG);
                    dOut.write(paramURI.getByteArray(), paramURI.getStartOffset(), paramURI.getLength());

                    // Separate the local name and prefix.
                    abvsParamQName.reset();
                    ICharacterIterator charIterator = new UTF8StringCharacterIterator(paramQName);
                    charIterator.reset();
                    int c = 0;
                    int prefixLength = 0;
                    while ((c = charIterator.next()) != ICharacterIterator.EOS_CHAR) {
                        if (c == Character.valueOf(':')) {
                            prefixLength = abvsParamQName.getLength();
                        } else {
                            dOutParamQName.writeChar(c);
                        }
                    }

                    dOut.write((byte) ((prefixLength >>> 8) & 0xFF));
                    dOut.write((byte) ((prefixLength >>> 0) & 0xFF));
                    dOut.write(abvsParamQName.getByteArray(), abvsParamQName.getStartOffset(), prefixLength);

                    int localNameLength = abvsParamQName.getLength() - prefixLength;
                    dOut.write((byte) ((localNameLength >>> 8) & 0xFF));
                    dOut.write((byte) ((localNameLength >>> 0) & 0xFF));
                    dOut.write(abvsParamQName.getByteArray(), abvsParamQName.getStartOffset() + prefixLength,
                            localNameLength);

                    result.set(abvs);
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}
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

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class FnSubstringEvaluatorFactory extends AbstractCharacterIteratorCopyingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSubstringEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    protected static int getIntParameter(final TaggedValuePointable tvp, final DoublePointable doublep,
            final LongPointable longp) throws SystemException {
        switch (tvp.getTag()) {
            case ValueTag.XS_INTEGER_TAG:
                tvp.getValue(longp);
                return longp.intValue();
            case ValueTag.XS_DOUBLE_TAG:
                tvp.getValue(doublep);
                // TODO Double needs to be rounded
                return doublep.intValue();
            default:
                throw new SystemException(ErrorCode.FORG0006);
        }
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SubstringCharacterIterator charIterator = new SubstringCharacterIterator(new UTF8StringCharacterIterator(
                stringp));
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

        return new AbstractCharacterIteratorCopyingEvaluator(args, charIterator) {
            @Override
            protected void preEvaluate(TaggedValuePointable[] args) throws SystemException {
                int startingLocation = 1;
                int length = Integer.MAX_VALUE;
                abvs.reset();
                charIterator.reset();

                // Only accept string, double, and optional double as input.
                TaggedValuePointable tvp1 = args[0];
                if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp1.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptyString(tvp);
                        tvp.getValue(stringp);
                    } else {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                } else {
                    if (!FunctionHelper.isDerivedFromString(tvp1.getTag())) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(stringp);
                }

                // TODO Check specification to see if only double? If so change passing function.
                startingLocation = getIntParameter(args[1], doublep, longp);

                // Third parameter may override default endingLoc.
                if (args.length > 2) {
                    length = getIntParameter(args[2], doublep, longp);
                }

                charIterator.setBounds(startingLocation, length);
            }
        };
    }
}

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
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class FnContainsEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnContainsEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final UTF8StringPointable stringp1 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp2 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp3 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringCharacterIterator charIterator1 = new UTF8StringCharacterIterator(stringp1);
        final UTF8StringCharacterIterator charIterator2 = new UTF8StringCharacterIterator(stringp2);
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                // Default result is false.
                byte[] booleanResult = new byte[2];
                booleanResult[0] = ValueTag.XS_BOOLEAN_TAG;
                booleanResult[1] = 0;

                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                // Only accept strings as input.
                if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp1.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptyString(stringp1);
                    } else {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                } else {
                    if (!FunctionHelper.isDerivedFromString(tvp1.getTag())) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(stringp1);
                }
                if (tvp2.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp2.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptyString(stringp2);
                    } else {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                } else {
                    if (!FunctionHelper.isDerivedFromString(tvp2.getTag())) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp2.getValue(stringp2);
                }
                charIterator1.reset();
                charIterator2.reset();

                // Third parameter is optional.
                if (args.length > 2) {
                    TaggedValuePointable tvp3 = args[2];
                    if (!FunctionHelper.isDerivedFromString(tvp3.getTag())) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp3.getValue(stringp3);
                }
                // TODO use the third value as collation

                // Only need to run comparisons if they both have a non empty string.
                if (stringp1.getUTF8Length() > 0 && stringp2.getUTF8Length() > 0) {
                    int c2 = charIterator2.next();
                    while (true) {
                        int c1 = charIterator1.next();
                        if (c1 == c2) {
                            int offset1 = charIterator1.getByteOffset();

                            // Check substring.
                            if (checkSubString(charIterator1, charIterator2)) {
                                booleanResult[1] = 1;
                                break;
                            }

                            // Reset for strings for continuation.
                            charIterator2.reset();
                            c2 = charIterator2.next();
                            charIterator1.setByteOffset(offset1);
                        }
                        if (c1 == ICharacterIterator.EOS_CHAR) {
                            // End of string and no match found.
                            break;
                        }
                    }
                } else if (stringp2.getUTF8Length() == 0) {
                    booleanResult[1] = 1;
                }

                result.set(booleanResult, 0, 2);
            }

            private boolean checkSubString(ICharacterIterator charIterator1, ICharacterIterator charIterator2) {
                while (true) {
                    int c1 = charIterator1.next();
                    int c2 = charIterator2.next();
                    if (c2 == ICharacterIterator.EOS_CHAR) {
                        // End of string.
                        return true;
                    }
                    if (c1 != c2) {
                        // No match found.
                        break;
                    }
                }
                return false;
            }
        };
    }
}

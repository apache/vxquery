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

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public abstract class AbstractCharacterIteratorCopyingEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    private static final int STRING_EXPECTED_LENGTH = 300;
    private final GrowableArray ga = new GrowableArray();
    private final UTF8StringBuilder sb = new UTF8StringBuilder();
    private final ICharacterIterator charIterator;
    private final ArrayBackedValueStorage abvs;

    public AbstractCharacterIteratorCopyingEvaluator(IScalarEvaluator[] args, ICharacterIterator charIterator) {
        super(args);
        this.charIterator = charIterator;
        abvs = new ArrayBackedValueStorage();
    }

    @Override
    public final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        preEvaluate(args);
        abvs.reset();
        charIterator.reset();
        try {
            // Byte Format: Type (1 byte) + String Length (X bytes) + String.
            DataOutput out = abvs.getDataOutput();
            out.write(ValueTag.XS_STRING_TAG);

            ga.reset();
            sb.reset(ga, STRING_EXPECTED_LENGTH);

            int c;
            while (ICharacterIterator.EOS_CHAR != (c = charIterator.next())) {
                FunctionHelper.writeChar((char) c, sb);
            }

            sb.finish();
            out.write(ga.getByteArray(), 0, ga.getLength());

            result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    protected abstract void preEvaluate(TaggedValuePointable[] args) throws SystemException;

}

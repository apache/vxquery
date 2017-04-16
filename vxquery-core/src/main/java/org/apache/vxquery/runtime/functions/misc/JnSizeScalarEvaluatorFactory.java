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
package org.apache.vxquery.runtime.functions.misc;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class JnSizeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    public JnSizeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        return new JnSizeScalarEvaluator(args);
    }

    private static class JnSizeScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();

        public JnSizeScalarEvaluator(IScalarEvaluator[] args) {
            super(args);
        }

        @Override
        protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            TaggedValuePointable tvp = args[0];
            if (!(tvp.getTag() != ValueTag.ARRAY_TAG || tvp.getTag() != ValueTag.OBJECT_TAG)) {
                throw new SystemException(ErrorCode.FORG0006);
            }
            abvs.reset();
            tvp.getValue(ap);
            DataOutput out = abvs.getDataOutput();
            ap.getEntryCount();
            try {
                out.write(ValueTag.XS_INTEGER_TAG);
                out.writeLong(ap.getEntryCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
            result.set(abvs);
        }
    }
}

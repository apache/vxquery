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
package org.apache.vxquery.runtime.functions.numeric;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public abstract class AbstractNumericScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractNumericScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractTaggedValueArgumentScalarEvaluator createEvaluator(IHyracksTaskContext ctx,
            IScalarEvaluator[] args) throws HyracksDataException {
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            final AbstractNumericOperation aOp = createNumericOperation();
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final ArrayBackedValueStorage abvsInteger = new ArrayBackedValueStorage();
            final DataOutput dOutInteger = abvsInteger.getDataOutput();
            final TypedPointables tp = new TypedPointables();

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp = args[0];
                int tid = getBaseTypeForArithmetics(tvp.getTag());
                abvs.reset();
                try {
                    switch (tid) {
                        case ValueTag.SEQUENCE_TAG:
                            tvp.getValue(tp.seqp);
                            if (tp.seqp.getEntryCount() == 0) {
                                XDMConstants.setEmptySequence(result);
                                return;
                            }

                        case ValueTag.XS_DECIMAL_TAG:
                            tvp.getValue(tp.decp);
                            aOp.operateDecimal(tp.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
                            switch (tvp.getTag()) {
                                case ValueTag.XS_INTEGER_TAG:
                                case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                                case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                                case ValueTag.XS_LONG_TAG:
                                case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                                case ValueTag.XS_UNSIGNED_LONG_TAG:
                                case ValueTag.XS_POSITIVE_INTEGER_TAG:
                                case ValueTag.XS_INT_TAG:
                                case ValueTag.XS_UNSIGNED_INT_TAG:
                                case ValueTag.XS_SHORT_TAG:
                                case ValueTag.XS_UNSIGNED_SHORT_TAG:
                                case ValueTag.XS_BYTE_TAG:
                                case ValueTag.XS_UNSIGNED_BYTE_TAG:
                                    abvsInteger.reset();
                                    FunctionHelper.getIntegerPointable(tvp, dOutInteger, tp);
                                    longp.set(abvsInteger.getByteArray(), abvsInteger.getStartOffset() + 1,
                                            LongPointable.TYPE_TRAITS.getFixedLength());
                            }
                            aOp.operateInteger(longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp.getValue(tp.floatp);
                            aOp.operateFloat(tp.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp.getValue(tp.doublep);
                            aOp.operateDouble(tp.doublep, dOut);
                            result.set(abvs);
                            return;
                    }
                } catch (SystemException se) {
                    throw se;
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                throw new SystemException(ErrorCode.XPTY0004);
            }

            private int getBaseTypeForArithmetics(int tid) throws SystemException {
                while (true) {
                    switch (tid) {
                        case ValueTag.XS_DECIMAL_TAG:
                        case ValueTag.XS_DOUBLE_TAG:
                        case ValueTag.XS_FLOAT_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                            return tid;

                        case ValueTag.XS_ANY_ATOMIC_TAG:
                            throw new SystemException(ErrorCode.XPTY0004);

                        default:
                            tid = BuiltinTypeRegistry.INSTANCE.getSchemaTypeById(tid).getBaseType().getTypeId();
                    }
                }
            }
        };
    }

    protected abstract AbstractNumericOperation createNumericOperation();
}

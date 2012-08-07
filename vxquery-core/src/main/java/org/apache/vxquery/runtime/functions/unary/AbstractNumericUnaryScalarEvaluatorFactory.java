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
package org.apache.vxquery.runtime.functions.unary;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.types.BuiltinTypeRegistry;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractNumericUnaryScalarEvaluatorFactory extends
        AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractNumericUnaryScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractTaggedValueArgumentScalarEvaluator createEvaluator(IHyracksTaskContext ctx,
            IScalarEvaluator[] args) throws AlgebricksException {
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            final AbstractNumericUnaryOperation aOp = createNumericUnaryOperation();
            final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            final DataOutput dOut = abvs.getDataOutput();
            final TypedPointables tp = new TypedPointables();

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                int tid = getBaseTypeForArithmetics(tvp1.getTag());
                abvs.reset();
                try {
                    switch (tid) {
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp1.getValue(tp.decp);
                            aOp.operateDecimal(tp.decp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_INTEGER_TAG:
                            tvp1.getValue(tp.longp);
                            aOp.operateInteger(tp.longp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp.floatp);
                            aOp.operateFloat(tp.floatp, dOut);
                            result.set(abvs);
                            return;

                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp.doublep);
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
                        case ValueTag.XS_LONG_TAG:
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

    private static class TypedPointables {
        DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
    }

    protected abstract AbstractNumericUnaryOperation createNumericUnaryOperation();
}
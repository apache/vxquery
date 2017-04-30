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
package org.apache.vxquery.runtime.functions.datetime;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class FnAdjustTimeToTimezoneScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnAdjustTimeToTimezoneScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        final XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        final XSDateTimePointable ctxDatetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                dCtx.getCurrentDateTime(ctxDatetimep);
                TaggedValuePointable tvp1 = args[0];
                if (tvp1.getTag() != ValueTag.XS_TIME_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(timep);

                // Second argument is optional and will used the dynamic context if not supplied.
                long tz;
                if (args.length == 2) {
                    TaggedValuePointable tvp2 = args[1];
                    if (tvp2.getTag() == ValueTag.XS_DAY_TIME_DURATION_TAG) {
                        tvp2.getValue(longp);
                        if (Math.abs(longp.getLong()) > DateTime.CHRONON_OF_HOUR * 14) {
                            throw new SystemException(ErrorCode.FODT0003);
                        }
                        tz = longp.getLong() / DateTime.CHRONON_OF_MINUTE;
                    } else {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                } else {
                    tz = ctxDatetimep.getTimezoneHour() * 60 + ctxDatetimep.getTimezoneMinute();
                }

                try {
                    abvs.reset();
                    abvsInner.reset();
                    // Convert to DateTime to help have a good date.
                    datetimep.set(abvsInner.getByteArray(), abvsInner.getStartOffset(),
                            XSDateTimePointable.TYPE_TRAITS.getFixedLength());
                    datetimep.setDateTime(1970, 1, 1, timep.getHour(), timep.getMinute(), timep.getMilliSecond(),
                            timep.getTimezoneHour(), timep.getTimezoneMinute());

                    DateTime.adjustDateTimeToTimezone(datetimep, tz, dOutInner);

                    byte[] bytes = abvsInner.getByteArray();
                    int startOffset = abvsInner.getStartOffset() + 1;
                    // Convert to time.
                    dOut.write(ValueTag.XS_TIME_TAG);
                    dOut.write(bytes, startOffset + XSDateTimePointable.HOUR_OFFSET,
                            XSTimePointable.TYPE_TRAITS.getFixedLength());

                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}

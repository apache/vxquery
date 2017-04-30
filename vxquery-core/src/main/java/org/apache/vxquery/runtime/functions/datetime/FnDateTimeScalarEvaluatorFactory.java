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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

public class FnDateTimeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnDateTimeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        final XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp1.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptySequence(result);
                        return;
                    }
                }
                if (tvp1.getTag() != ValueTag.XS_DATE_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(datep);

                TaggedValuePointable tvp2 = args[1];
                if (tvp2.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp2.getValue(seqp);
                    if (seqp.getEntryCount() == 0) {
                        XDMConstants.setEmptySequence(result);
                        return;
                    }
                }
                if (tvp2.getTag() != ValueTag.XS_TIME_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp2.getValue(timep);

                // Set the timezone.
                byte timezoneHour, timezoneMinute;
                if (datep.getTimezoneHour() == DateTime.TIMEZONE_HOUR_NULL
                        && datep.getTimezoneMinute() == DateTime.TIMEZONE_MINUTE_NULL
                        && timep.getTimezoneHour() == DateTime.TIMEZONE_HOUR_NULL
                        && timep.getTimezoneMinute() == DateTime.TIMEZONE_MINUTE_NULL) {
                    // both null.
                    timezoneHour = DateTime.TIMEZONE_HOUR_NULL;
                    timezoneMinute = DateTime.TIMEZONE_MINUTE_NULL;
                } else if (datep.getTimezoneHour() == DateTime.TIMEZONE_HOUR_NULL
                        && datep.getTimezoneMinute() == DateTime.TIMEZONE_MINUTE_NULL
                        && timep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                        && timep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL) {
                    // date is null.
                    timezoneHour = (byte) timep.getTimezoneHour();
                    timezoneMinute = (byte) timep.getTimezoneMinute();
                } else if (datep.getTimezoneHour() != DateTime.TIMEZONE_HOUR_NULL
                        && datep.getTimezoneMinute() != DateTime.TIMEZONE_MINUTE_NULL
                        && timep.getTimezoneHour() == DateTime.TIMEZONE_HOUR_NULL
                        && timep.getTimezoneMinute() == DateTime.TIMEZONE_MINUTE_NULL) {
                    // time is null.
                    timezoneHour = (byte) datep.getTimezoneHour();
                    timezoneMinute = (byte) datep.getTimezoneMinute();
                } else if (datep.getTimezoneHour() == timep.getTimezoneHour()
                        && datep.getTimezoneMinute() == timep.getTimezoneMinute()) {
                    // timezones are the same.
                    timezoneHour = (byte) datep.getTimezoneHour();
                    timezoneMinute = (byte) datep.getTimezoneMinute();
                } else {
                    // Neither match.
                    throw new SystemException(ErrorCode.FORG0008);
                }

                try {
                    abvs.reset();
                    dOut.write(ValueTag.XS_DATETIME_TAG);
                    dOut.write(datep.getByteArray(), datep.getStartOffset(),
                            XSDatePointable.TYPE_TRAITS.getFixedLength() - 2);
                    dOut.write(timep.getByteArray(), timep.getStartOffset(),
                            XSTimePointable.TYPE_TRAITS.getFixedLength() - 2);
                    dOut.write(timezoneHour);
                    dOut.write(timezoneMinute);

                    result.set(abvs.getByteArray(), abvs.getStartOffset(),
                            XSDateTimePointable.TYPE_TRAITS.getFixedLength() + 1);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}

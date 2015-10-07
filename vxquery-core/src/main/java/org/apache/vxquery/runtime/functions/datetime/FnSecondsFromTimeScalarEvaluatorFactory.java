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

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnSecondsFromTimeScalarEvaluatorFactory extends AbstractValueFromDateTimeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSecondsFromTimeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected int getInputTag() {
        return ValueTag.XS_TIME_TAG;
    }

    @Override
    protected int getReturnTag() {
        return ValueTag.XS_DECIMAL_TAG;
    }

    @Override
    protected long getValueAsInteger(TaggedValuePointable tvp) {
        XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        tvp.getValue(timep);
        return timep.getMilliSecond();
    }
}

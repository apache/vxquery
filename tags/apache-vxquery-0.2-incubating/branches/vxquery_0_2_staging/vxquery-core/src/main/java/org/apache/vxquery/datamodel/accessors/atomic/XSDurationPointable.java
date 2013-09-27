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
package org.apache.vxquery.datamodel.accessors.atomic;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class XSDurationPointable extends AbstractPointable {
    private final static int YEAR_MONTH_OFFSET = 0;
    private final static int DAY_TIME_OFFSET = 4;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 12;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new XSDurationPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static int getYearMonth(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start + YEAR_MONTH_OFFSET);
    }

    public int getYearMonth() {
        return getYearMonth(bytes, start);
    }

    public static long getDayTime(byte[] bytes, int start) {
        return LongPointable.getLong(bytes, start + DAY_TIME_OFFSET);
    }

    public long getDayTime() {
        return getDayTime(bytes, start);
    }

    public static void setYearMonth(byte[] bytes, int start, int value) {
        IntegerPointable.setInteger(bytes, start + YEAR_MONTH_OFFSET, value);
    }

    public void setYearMonth(int value) {
        setYearMonth(bytes, start, value);
    }

    public static void setDayTime(byte[] bytes, int start, long value) {
        LongPointable.setLong(bytes, start + DAY_TIME_OFFSET, value);
    }

    public void setDayTime(long value) {
        setDayTime(bytes, start, value);
    }
}
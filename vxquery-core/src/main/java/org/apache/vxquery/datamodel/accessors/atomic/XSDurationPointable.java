package org.apache.vxquery.datamodel.accessors.atomic;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

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
            return 8;
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

    public static int getDayTime(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start + DAY_TIME_OFFSET);
    }

    public int getDayTime() {
        return getDayTime(bytes, start);
    }

    public static void setYearMonth(byte[] bytes, int start, int value) {
        IntegerPointable.setInteger(bytes, start + YEAR_MONTH_OFFSET, value);
    }
    
    public void setYearMonth(int value) {
        setYearMonth(bytes, start, value);
    }
    
    public static void setDayTime(byte[] bytes, int start, int value) {
        IntegerPointable.setInteger(bytes, start + DAY_TIME_OFFSET, value);
    }

    public void setDayTime(int value) {
        setDayTime(bytes, start, value);
    }
}
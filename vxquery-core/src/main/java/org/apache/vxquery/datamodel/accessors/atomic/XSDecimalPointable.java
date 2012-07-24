package org.apache.vxquery.datamodel.accessors.atomic;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class XSDecimalPointable extends AbstractPointable implements IHashable, IComparable, INumeric {
    private final static int DECIMAL_PLACE_OFFSET = 0;
    private final static int VALUE_OFFSET = 1;
    public final static int PRECISION = 18;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 9;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new XSDecimalPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        long v = getDecimalValue();
        byte p = getDecimalPlace();
        long ov = getDecimalValue(bytes, start);
        byte op = getDecimalPlace(bytes, start);

        // Make both long values have the decimal point at the same place. 
        // TODO double check that precision is not being lost.
        int diff = p - op;
        if (diff > 0) {
            v = Math.round(v / Math.pow(10, diff));
        } else if (diff < 0) {
            ov = Math.round(ov / Math.pow(10, diff));
        }

        return v < ov ? -1 : (v > ov ? 1 : 0);
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    public void setDecimal(long value, byte decimalPlace) {
        BytePointable.setByte(bytes, start + DECIMAL_PLACE_OFFSET, decimalPlace);
        LongPointable.setLong(bytes, start + VALUE_OFFSET, value);
        normalize();
    }

    public void setDecimal(double doubleValue) {
        setDecimal(doubleValue, bytes, start);
    }

    public static void setDecimal(double doubleValue, byte[] bytes, int start) {
        byte decimalPlace = 0;
        long value = 0;
        boolean pastDecimal = false;
        int count = 0;
        int c;
        Double doubleObject = new Double(doubleValue);
        String strTest = doubleObject.toString();

        for (int i = 0; i < strTest.length() && count < PRECISION; ++i) {
            c = strTest.charAt(i);
            if (Character.isDigit(c)) {
                value = value * 10 + Character.getNumericValue(c);
                if (pastDecimal) {
                    decimalPlace++;
                }
                count++;
            } else {
                pastDecimal = true;
            }
        }

        BytePointable.setByte(bytes, start + DECIMAL_PLACE_OFFSET, decimalPlace);
        LongPointable.setLong(bytes, start + VALUE_OFFSET, value);
        normalize(bytes, start);
    }

    public void normalize() {
        normalize(bytes, start);
    }

    public static void normalize(byte[] bytes, int start) {
        byte decimalPlace = getDecimalPlace(bytes, start);
        long value = getDecimalValue(bytes, start);
        // Normalize the value and take off trailing zeros.
        while (value != 0 && value % 10 == 0) {
            value -= 10;
            --decimalPlace;
        }
        BytePointable.setByte(bytes, start + DECIMAL_PLACE_OFFSET, decimalPlace);
        LongPointable.setLong(bytes, start + VALUE_OFFSET, value);
    }

    public byte getDecimalPlace() {
        return BytePointable.getByte(bytes, start + DECIMAL_PLACE_OFFSET);
    }

    public static byte getDecimalPlace(byte[] bytes, int start) {
        return BytePointable.getByte(bytes, start + DECIMAL_PLACE_OFFSET);
    }

    public long getDecimalValue() {
        return LongPointable.getLong(bytes, start + VALUE_OFFSET);
    }

    public static long getDecimalValue(byte[] bytes, int start) {
        return LongPointable.getLong(bytes, start + VALUE_OFFSET);
    }

    @Override
    public int hash() {
        long v = getDecimalValue();
        return (int) (v ^ (v >>> 32));
    }

    public long getBeforeDecimalPlace() {
        return getBeforeDecimalPlace(bytes, start);
    }

    public static long getBeforeDecimalPlace(byte[] bytes, int start) {
        return Math.round(getDecimalValue(bytes, start) / Math.pow(10, getDecimalPlace(bytes, start)));
    }

    public byte getDigitCount() {
        return getDigitCount(bytes, start);
    }

    public static byte getDigitCount(byte[] bytes, int start) {
        return (byte) (Math.log10((double) getDecimalValue(bytes, start)) + 1);
    }

    @Override
    public byte byteValue() {
        return (byte) getBeforeDecimalPlace();
    }

    @Override
    public short shortValue() {
        return (short) getBeforeDecimalPlace();
    }

    @Override
    public int intValue() {
        return (int) getBeforeDecimalPlace();
    }

    @Override
    public long longValue() {
        return getBeforeDecimalPlace();
    }

    @Override
    public float floatValue() {
        return (float) (getDecimalValue() / Math.pow(10, getDecimalPlace()));
    }

    @Override
    public double doubleValue() {
        return getDecimalValue() / Math.pow(10, getDecimalPlace());
    }

}
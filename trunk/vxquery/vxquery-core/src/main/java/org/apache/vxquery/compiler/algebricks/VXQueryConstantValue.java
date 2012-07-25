package org.apache.vxquery.compiler.algebricks;

import org.apache.commons.codec.binary.Hex;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class VXQueryConstantValue implements IAlgebricksConstantValue {
    private final SequenceType type;

    private final byte[] value;

    public VXQueryConstantValue(SequenceType type, byte[] value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public boolean isFalse() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isTrue() {
        return false;
    }

    public SequenceType getType() {
        return type;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(type).append("(bytes[").append(value.length).append("] = [").append(Hex.encodeHexString(value))
                .append("])");
        return buffer.toString();
    }
}
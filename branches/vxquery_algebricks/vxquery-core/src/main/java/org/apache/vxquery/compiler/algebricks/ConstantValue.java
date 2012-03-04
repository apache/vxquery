package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class ConstantValue implements IAlgebricksConstantValue {
    private final SequenceType type;

    private final Object value;

    public ConstantValue(SequenceType type, Object value) {
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

    public Object getValue() {
        return value;
    }
}
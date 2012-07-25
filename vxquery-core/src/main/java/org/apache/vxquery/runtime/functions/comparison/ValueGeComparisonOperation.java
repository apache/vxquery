package org.apache.vxquery.runtime.functions.comparison;

public class ValueGeComparisonOperation extends AbstractNegatingComparisonOperation {

    @Override
    protected AbstractValueComparisonOperation createBaseComparisonOperation() {
        return new ValueLtComparisonOperation();
    }

}
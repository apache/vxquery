package org.apache.vxquery.runtime.functions.comparison;


public class ValueNeComparisonOperation extends AbstractNegatingComparisonOperation {
    
    @Override
    protected AbstractValueComparisonOperation createBaseComparisonOperation() {
        return new ValueEqComparisonOperation();
    }

}
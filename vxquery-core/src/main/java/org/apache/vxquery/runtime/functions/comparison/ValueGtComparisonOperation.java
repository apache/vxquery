package org.apache.vxquery.runtime.functions.comparison;


public class ValueGtComparisonOperation extends AbstractNegatingComparisonOperation {
    
    @Override
    protected AbstractValueComparisonOperation createBaseComparisonOperation() {
        return new ValueLeComparisonOperation();
    }

}
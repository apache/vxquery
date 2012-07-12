package org.apache.vxquery.runtime.functions.comparison;


public class ValueLeComparisonOperation extends AbstractDisjunctiveComparisonOperation {

    @Override
    protected AbstractValueComparisonOperation createBaseComparisonOperation1() {
        return new ValueLtComparisonOperation();
    }

    @Override
    protected AbstractValueComparisonOperation createBaseComparisonOperation2() {
        return new ValueEqComparisonOperation();
    }

}
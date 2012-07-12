package org.apache.vxquery.runtime.functions.comparison;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class ValueLeComparisonScalarEvaluatorFactory extends AbstractValueComparisonScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public ValueLeComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractValueComparisonOperation createValueComparisonOperation() {
        return new ValueLeComparisonOperation();
    }
}
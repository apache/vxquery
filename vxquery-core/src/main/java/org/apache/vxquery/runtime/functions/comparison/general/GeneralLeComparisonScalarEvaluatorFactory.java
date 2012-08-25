package org.apache.vxquery.runtime.functions.comparison.general;

import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.comparison.ValueLeComparisonOperation;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class GeneralLeComparisonScalarEvaluatorFactory extends AbstractGeneralComparisonScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public GeneralLeComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractValueComparisonOperation createValueComparisonOperation() {
        return new ValueLeComparisonOperation();
    }
}
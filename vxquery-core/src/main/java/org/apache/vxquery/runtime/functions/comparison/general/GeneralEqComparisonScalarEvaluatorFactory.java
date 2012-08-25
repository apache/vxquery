package org.apache.vxquery.runtime.functions.comparison.general;

import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.comparison.ValueEqComparisonOperation;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class GeneralEqComparisonScalarEvaluatorFactory extends AbstractGeneralComparisonScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public GeneralEqComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractValueComparisonOperation createValueComparisonOperation() {
        return new ValueEqComparisonOperation();
    }
}
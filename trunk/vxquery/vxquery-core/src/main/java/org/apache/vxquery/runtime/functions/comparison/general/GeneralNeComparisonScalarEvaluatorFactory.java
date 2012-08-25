package org.apache.vxquery.runtime.functions.comparison.general;

import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.comparison.ValueNeComparisonOperation;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class GeneralNeComparisonScalarEvaluatorFactory extends AbstractGeneralComparisonScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public GeneralNeComparisonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractValueComparisonOperation createValueComparisonOperation() {
        return new ValueNeComparisonOperation();
    }
}
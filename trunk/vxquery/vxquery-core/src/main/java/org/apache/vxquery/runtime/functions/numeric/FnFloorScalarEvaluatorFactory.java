package org.apache.vxquery.runtime.functions.numeric;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnFloorScalarEvaluatorFactory extends AbstractNumericScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnFloorScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractNumericOperation createNumericOperation() {
        return new FnFloorOperation();
    }
}
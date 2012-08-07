package org.apache.vxquery.runtime.functions.unary;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class NumericUnaryPlusScalarEvaluatorFactory extends AbstractNumericUnaryScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public NumericUnaryPlusScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected AbstractNumericUnaryOperation createNumericUnaryOperation() {
        return new NumericUnaryMinusOperation();
    }
}
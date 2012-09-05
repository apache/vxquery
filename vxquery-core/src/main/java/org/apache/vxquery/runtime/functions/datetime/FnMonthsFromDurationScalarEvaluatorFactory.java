package org.apache.vxquery.runtime.functions.datetime;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnMonthsFromDurationScalarEvaluatorFactory extends AbstractValueFromDurationScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnMonthsFromDurationScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected long convertDuration(long YMDuration, long DTDuration) {
        return (YMDuration % 12);
    }

}

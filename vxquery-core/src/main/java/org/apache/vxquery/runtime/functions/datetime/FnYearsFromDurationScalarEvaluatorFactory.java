package org.apache.vxquery.runtime.functions.datetime;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnYearsFromDurationScalarEvaluatorFactory extends AbstractValueFromDurationScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnYearsFromDurationScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected long convertDuration(long YMDuration, long DTDuration) {
        return (YMDuration / 12);
    }

}

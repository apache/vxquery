package org.apache.vxquery.runtime.functions.datetime;

import org.apache.vxquery.datamodel.util.DateTime;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnDaysFromDurationScalarEvaluatorFactory extends AbstractValueFromDurationScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnDaysFromDurationScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected long convertDuration(long YMDuration, long DTDuration) {
        return (DTDuration / DateTime.CHRONON_OF_DAY);
    }

}

package org.apache.vxquery.runtime.functions.datetime;

import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnSecondsFromDurationScalarEvaluatorFactory extends AbstractValueFromDurationScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSecondsFromDurationScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected long convertDuration(long YMDuration, long DTDuration) {
        return (((DTDuration % DateTime.CHRONON_OF_DAY) % DateTime.CHRONON_OF_HOUR) % DateTime.CHRONON_OF_MINUTE);
    }

    @Override
    protected int getReturnTag() {
        return ValueTag.XS_DECIMAL_TAG;
    }

}

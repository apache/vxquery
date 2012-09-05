package org.apache.vxquery.runtime.functions.datetime;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnTimezoneFromTimeScalarEvaluatorFactory extends AbstractValueFromDateTimeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    final XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();

    public FnTimezoneFromTimeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected int getInputTag() {
        return ValueTag.XS_TIME_TAG;
    }

    @Override
    protected int getReturnTag() {
        return ValueTag.XS_DAY_TIME_DURATION_TAG;
    }

    @Override
    protected long getInteger(TaggedValuePointable tvp) {
        tvp.getValue(timep);
        return getTimezone(timep);
    }
}

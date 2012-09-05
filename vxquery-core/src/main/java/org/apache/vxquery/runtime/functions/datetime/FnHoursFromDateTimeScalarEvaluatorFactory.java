package org.apache.vxquery.runtime.functions.datetime;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnHoursFromDateTimeScalarEvaluatorFactory extends AbstractValueFromDateTimeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private static final XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY
            .createPointable();

    public FnHoursFromDateTimeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected int getInputTag() {
        return ValueTag.XS_DATETIME_TAG;
    }

    @Override
    protected long getInteger(TaggedValuePointable tvp) {
        tvp.getValue(datetimep);
        return datetimep.getHour();
    }
}

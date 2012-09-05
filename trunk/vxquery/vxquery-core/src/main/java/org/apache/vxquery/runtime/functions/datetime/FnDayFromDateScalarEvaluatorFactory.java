package org.apache.vxquery.runtime.functions.datetime;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnDayFromDateScalarEvaluatorFactory extends AbstractValueFromDateTimeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    final XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();

    public FnDayFromDateScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected int getInputTag() {
        return ValueTag.XS_DATE_TAG;
    }

    @Override
    protected long getInteger(TaggedValuePointable tvp) {
        tvp.getValue(datep);
        return datep.getDay();
    }
}

package org.apache.vxquery.runtime.functions.datetime;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnYearFromDateScalarEvaluatorFactory extends AbstractValueFromDateTimeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private static final XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();

    public FnYearFromDateScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected int getInputTag() {
        return ValueTag.XS_DATE_TAG;
    }

    @Override
    protected long getInteger(TaggedValuePointable tvp) {
        tvp.getValue(datep);
        return datep.getYear();
    }
}

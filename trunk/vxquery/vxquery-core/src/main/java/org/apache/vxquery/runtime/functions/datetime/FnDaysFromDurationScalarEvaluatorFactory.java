package org.apache.vxquery.runtime.functions.datetime;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.util.DateTime;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnDaysFromDurationScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnDaysFromDurationScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                long value;
                if (tvp1.getTag() == ValueTag.XS_DURATION_TAG) {
                    tvp1.getValue(durationp);
                    value = durationp.getDayTime();
                } else if (tvp1.getTag() == ValueTag.XS_DAY_TIME_DURATION_TAG) {
                    tvp1.getValue(longp);
                    value = longp.getLong();
                } else if (tvp1.getTag() == ValueTag.XS_YEAR_MONTH_DURATION_TAG) {
                    value = 0;
                } else {
                    throw new SystemException(ErrorCode.FORG0006);
                }

                value = value / DateTime.CHRONON_OF_DAY;

                try {
                    abvsInner.reset();
                    dOutInner.write(ValueTag.XS_INTEGER_TAG);
                    dOutInner.writeLong(value);
                    result.set(abvsInner);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}

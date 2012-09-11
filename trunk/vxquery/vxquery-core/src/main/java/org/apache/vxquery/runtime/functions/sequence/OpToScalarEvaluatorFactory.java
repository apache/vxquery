package org.apache.vxquery.runtime.functions.sequence;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
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

public class OpToScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public OpToScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();
        final SequenceBuilder sb = new SequenceBuilder();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    TaggedValuePointable tvp1 = args[0];
                    if (tvp1.getTag() != ValueTag.XS_INTEGER_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(longp);
                    long start = longp.getLong();

                    TaggedValuePointable tvp2 = args[1];
                    if (tvp2.getTag() != ValueTag.XS_INTEGER_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp2.getValue(longp);
                    long end = longp.getLong();

                    abvs.reset();
                    sb.reset(abvs);
                    if (start > end) {
                        XDMConstants.setEmptySequence(result);
                        return;
                    } else {
                        for (long j = start; j <= end; ++j) {
                            abvsInner.reset();
                            dOutInner.write(ValueTag.XS_INTEGER_TAG);
                            dOutInner.writeLong(j);
                            sb.addItem(abvsInner);
                        }
                    }
                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
            }
        };
    }
}
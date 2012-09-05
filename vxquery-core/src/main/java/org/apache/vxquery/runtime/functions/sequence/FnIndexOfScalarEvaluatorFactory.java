package org.apache.vxquery.runtime.functions.sequence;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.comparison.ValueEqComparisonOperation;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnIndexOfScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnIndexOfScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = new SequencePointable();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final AbstractValueComparisonOperation aOp = new ValueEqComparisonOperation();
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];

                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        for (int j = 0; j < seqLen; ++j) {
                            seq.getEntry(j, p);
                            tvp.set(p.getByteArray(), p.getStartOffset(), p.getLength());
                            if (AbstractValueComparisonScalarEvaluatorFactory.compareTaggedValues(aOp, tvp, tvp2, dCtx)) {
                                abvsInner.reset();
                                dOutInner.write(ValueTag.XS_INTEGER_TAG);
                                dOutInner.writeLong(j + 1);
                                sb.addItem(abvsInner);
                            }
                        }
                    } else {
                        if (AbstractValueComparisonScalarEvaluatorFactory.compareTaggedValues(aOp, tvp1, tvp2, dCtx)) {
                            abvsInner.reset();
                            dOutInner.write(ValueTag.XS_INTEGER_TAG);
                            dOutInner.writeLong(1);
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
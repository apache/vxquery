package org.apache.vxquery.runtime.functions.sequence;

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
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
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnRemoveScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnRemoveScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = new SequencePointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    TaggedValuePointable tvp2 = args[1];
                    if (tvp2.getTag() != ValueTag.XS_INTEGER_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp2.getValue(longp);

                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp1 = args[0];
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        if (longp.getLong() < 1 || longp.getLong() > seqLen) {
                            // Position is outside the sequence. Return target.
                            result.set(tvp1);
                            return;
                        } else {
                            for (int j = 0; j < seqLen; ++j) {
                                if (longp.getLong() != j + 1) {
                                    seq.getEntry(j, p);
                                    sb.addItem(p);
                                }
                            }
                        }
                    } else if (longp.getLong() != 1) {
                        // Position does not match the item. Return target.
                        result.set(tvp1);
                        return;
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
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
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnReverseScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnReverseScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = new SequencePointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    TaggedValuePointable tvp = args[0];
                    if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                        abvs.reset();
                        sb.reset(abvs);
                        tvp.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        for (int j = 0; j < seqLen; ++j) {
                            seq.getEntry(seqLen - j - 1, p);
                            sb.addItem(p);
                        }
                        sb.finish();
                        result.set(abvs);
                    } else {
                        result.set(tvp);
                    }
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
            }
        };
    }
}
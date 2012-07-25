package org.apache.vxquery.runtime.functions.sequence;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.data.std.api.IPointable;

public class IterateUnnestingEvaluatorFactory extends AbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public IterateUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final SequencePointable seqp = new SequencePointable();
        return new AbstractTaggedValueArgumentUnnestingEvaluator(args) {
            private int index;
            private int seqLength;

            @Override
            public boolean step(IPointable result) throws AlgebricksException {
                TaggedValuePointable tvp = tvps[0];
                if (tvp.getTag() != ValueTag.SEQUENCE_TAG) {
                    if (index == 0) {
                        result.set(tvp);
                        ++index;
                        return true;
                    }
                } else {
                    if (index < seqLength) {
                        seqp.getEntry(index, result);
                        ++index;
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected void init(TaggedValuePointable[] args) {
                index = 0;
                TaggedValuePointable tvp = tvps[0];
                if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                    tvp.getValue(seqp);
                    seqLength = seqp.getEntryCount();
                }
            }
        };
    }
}
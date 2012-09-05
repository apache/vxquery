package org.apache.vxquery.runtime.functions.sequence;

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.numeric.FnRoundOperation;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnSubsequenceScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSubsequenceScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();
        final SequencePointable seq = new SequencePointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();
        final DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvsRound = new ArrayBackedValueStorage();
        final FnRoundOperation round = new FnRoundOperation();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    long startingLoc;
                    TaggedValuePointable tvp2 = args[1];
                    // XQuery Specification calls for double value. Integer and Decimal are allowed to cut down
                    // on casting.
                    if (tvp2.getTag() == ValueTag.XS_DOUBLE_TAG) {
                        tvp2.getValue(doublep);
                        abvsRound.reset();
                        round.operateDouble(doublep, abvsRound.getDataOutput());
                        doublep.set(abvsRound.getByteArray(), abvsRound.getStartOffset() + 1,
                                DoublePointable.TYPE_TRAITS.getFixedLength());
                        startingLoc = doublep.longValue();
                    } else if (tvp2.getTag() == ValueTag.XS_INTEGER_TAG) {
                        tvp2.getValue(longp);
                        startingLoc = longp.longValue();
                    } else if (tvp2.getTag() == ValueTag.XS_DECIMAL_TAG) {
                        tvp2.getValue(decp);
                        startingLoc = decp.longValue();
                    } else {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    if (startingLoc < 1) {
                        startingLoc = 1;
                    }

                    // Get length.
                    long endingLoc = Long.MAX_VALUE;
                    if (args.length > 2) {
                        TaggedValuePointable tvp3 = args[2];
                        // XQuery Specification calls for double value. Integer and Decimal are allowed to cut down
                        // on casting.
                        if (tvp3.getTag() == ValueTag.XS_DOUBLE_TAG) {
                            tvp3.getValue(doublep);
                            abvsRound.reset();
                            round.operateDouble(doublep, abvsRound.getDataOutput());
                            doublep.set(abvsRound.getByteArray(), abvsRound.getStartOffset() + 1,
                                    DoublePointable.TYPE_TRAITS.getFixedLength());
                            endingLoc = startingLoc + doublep.longValue();
                        } else if (tvp3.getTag() == ValueTag.XS_INTEGER_TAG) {
                            tvp3.getValue(longp);
                            endingLoc = startingLoc + longp.longValue();
                        } else if (tvp3.getTag() == ValueTag.XS_DECIMAL_TAG) {
                            tvp3.getValue(decp);
                            endingLoc = startingLoc + decp.longValue();
                        } else {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                    }

                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp1 = args[0];
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        if (endingLoc < startingLoc) {
                            // Empty sequence.
                        } else if (startingLoc == 1 && endingLoc > seqLen) {
                            // Includes whole sequence.
                            result.set(tvp1);
                            return;
                        } else {
                            for (int j = 0; j < seqLen; ++j) {
                                if (startingLoc <= j + 1 && j + 1 < endingLoc) {
                                    seq.getEntry(j, p);
                                    sb.addItem(p);
                                }
                            }
                        }
                    } else if (startingLoc == 1 && endingLoc > 1) {
                        // Includes item.
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
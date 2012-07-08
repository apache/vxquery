package org.apache.vxquery.runtime.functions.sequence;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.util.GrowableIntArray;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class ConcatenateScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public ConcatenateScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final GrowableIntArray slots = new GrowableIntArray();
        final ArrayBackedValueStorage dataArea = new ArrayBackedValueStorage();
        final SequencePointable seq = new SequencePointable();
        final VoidPointable p = (VoidPointable) VoidPointable.FACTORY.createPointable();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                abvs.reset();
                slots.clear();
                dataArea.reset();
                for (int i = 0; i < args.length; ++i) {
                    TaggedValuePointable tvp = args[i];
                    if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp.getValue(seq);
                        int seqLen = seq.getEntryCount();
                        for (int j = 0; j < seqLen; ++j) {
                            seq.getEntry(j, p);
                            addItem(p);
                        }
                    } else {
                        addItem(tvp);
                    }
                }
                if (slots.getSize() != 1) {
                    assembleResult(abvs, slots, dataArea);
                    result.set(abvs);
                } else {
                    result.set(dataArea);
                }
            }

            private void assembleResult(ArrayBackedValueStorage abvs, GrowableIntArray slots,
                    ArrayBackedValueStorage dataArea) throws SystemException {
                try {
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.SEQUENCE_TAG);
                    int size = slots.getSize();
                    out.writeInt(size);
                    if (size > 0) {
                        int[] slotArray = slots.getArray();
                        for (int i = 0; i < size; ++i) {
                            out.writeInt(slotArray[i]);
                        }
                        out.write(dataArea.getByteArray(), dataArea.getStartOffset(), dataArea.getLength());
                    }
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            private void addItem(final IPointable p) throws SystemException {
                try {
                    dataArea.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
                    slots.append(dataArea.getLength());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}
package org.apache.vxquery.runtime.functions.strings;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
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
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class FnConcatEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnConcatEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp = new UTF8StringPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                abvs.reset();

                try {
                    // Byte Format: Type (1 byte) + String Byte Length (2 bytes) + String.
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.XS_STRING_TAG);

                    // Default values for the length and update later
                    out.write(0xFF);
                    out.write(0xFF);

                    for (int i = 0; i < args.length; i++) {
                        TaggedValuePointable tvp = args[i];

                        // TODO Update function to support cast to a string from any atomic value.
                        if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
                            throw new SystemException(ErrorCode.FORG0006);
                        }

                        tvp.getValue(stringp);
                        out.write(stringp.getByteArray(), stringp.getStartOffset() + 2, stringp.getUTFLength());
                    }

                    // Update the full length string in the byte array.
                    byte[] stringResult = abvs.getByteArray();
                    stringResult[1] = (byte) (((abvs.getLength() - 3) >>> 8) & 0xFF);
                    stringResult[2] = (byte) (((abvs.getLength() - 3) >>> 0) & 0xFF);

                    result.set(stringResult, 0, abvs.getLength());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}

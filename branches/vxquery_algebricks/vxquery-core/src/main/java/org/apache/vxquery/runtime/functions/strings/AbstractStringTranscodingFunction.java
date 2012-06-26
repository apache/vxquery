package org.apache.vxquery.runtime.functions.strings;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public abstract class AbstractStringTranscodingFunction extends AbstractTaggedValueArgumentScalarEvaluator {
    final UTF8StringPointable stringp = new UTF8StringPointable();
    final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

    public AbstractStringTranscodingFunction(IScalarEvaluator[] args) {
        super(args);
    }

    @Override
    final protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        int c = 0;

        abvs.reset();
        TaggedValuePointable tvp1 = args[0];

        // Only accept strings as input.
        if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
            throw new SystemException(ErrorCode.FORG0006);
        }

        tvp1.getValue(stringp);
        int length = stringp.getStringLength();

        try {
            // Byte Format: Type (1 byte) + String Length (2 bytes) + String.
            DataOutput out = abvs.getDataOutput();
            out.write(ValueTag.XS_STRING_TAG);

            // Default values for the length and update later
            out.write(0xFF);
            out.write(0xFF);

            int i = 0;
            int offset = 2;
            // General case for characters.
            for (; i < length; ++i) {
                c = transcodeCharacter(stringp.charAt(offset));
                if (!((c >= 0x0001) && (c <= 0x007F))) {
                    break;
                }
                out.write((byte) c);
                ++offset;
            }

            // Uncommon case: Must look at full range of characters.
            for (; i < length; ++i) {
                c = transcodeCharacter(stringp.charAt(offset));
                if ((c >= 0x0001) && (c <= 0x007F)) {
                    out.write((byte) c);
                    ++offset;
                } else if (c > 0x07FF) {
                    out.write((byte) (0xE0 | ((c >> 12) & 0x0F)));
                    out.write((byte) (0x80 | ((c >> 6) & 0x3F)));
                    out.write((byte) (0x80 | ((c >> 0) & 0x3F)));
                    offset += 3;
                } else {
                    out.write((byte) (0xC0 | ((c >> 6) & 0x1F)));
                    out.write((byte) (0x80 | ((c >> 0) & 0x3F)));
                    offset += 2;
                }
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

    protected abstract char transcodeCharacter(char c);

}

package org.apache.vxquery.runtime.functions.json;

import java.io.IOException;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.AbstractForwardAxisPathStep;

public class KeysOrMembersUnnesting extends AbstractForwardAxisPathStep {
    private final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
    private final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private final ArrayBuilder ab = new ArrayBuilder();
    private int arrayArgsLength;
    private int indexArrayArgs;

    public KeysOrMembersUnnesting(IHyracksTaskContext ctx, PointablePool pp) {
        super(ctx, pp);
    }

    protected void init(TaggedValuePointable[] args) {
        indexArrayArgs = 0;
        if (args[0].getTag() == ValueTag.ARRAY_TAG) {
            args[0].getValue(ap);
            arrayArgsLength = ap.getEntryCount();
        }
    }

    public boolean step(IPointable result) throws SystemException {
        if (arrayArgsLength > 0) {
            while (indexArrayArgs < arrayArgsLength) {
                ap.getEntry(indexArrayArgs, tvp);
                abvs.reset();
                try {
                    ab.reset(abvs);
                    ab.addItem(tvp);
                    ab.finish();
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                result.set(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength());
                indexArrayArgs++;
                return true;
            }
        }
        return false;
    }
}

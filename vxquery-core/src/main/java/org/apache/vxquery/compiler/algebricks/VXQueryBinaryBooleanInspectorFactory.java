package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;

public class VXQueryBinaryBooleanInspectorFactory implements IBinaryBooleanInspectorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IBinaryBooleanInspector createBinaryBooleanInspector(IHyracksTaskContext ctx) {
        final TaggedValuePointable tvp = new TaggedValuePointable();
        final BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        return new IBinaryBooleanInspector() {
            @Override
            public boolean getBooleanValue(byte[] bytes, int offset, int length) {
                tvp.set(bytes, offset, length);
                assert tvp.getTag() == ValueTag.XS_BOOLEAN_TAG;
                tvp.getValue(bp);
                return bp.getBoolean();
            }
        };
    }
}
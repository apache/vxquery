package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class VXQueryBinaryIntegerInspectorFactory implements IBinaryIntegerInspectorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IBinaryIntegerInspector createBinaryIntegerInspector(IHyracksTaskContext ctx) {
        final TaggedValuePointable tvp = new TaggedValuePointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        return new IBinaryIntegerInspector() {
            @Override
            public int getIntegerValue(byte[] bytes, int offset, int length) {
                tvp.set(bytes, offset, length);
                assert tvp.getTag() == ValueTag.XS_INT_TAG;
                tvp.getValue(ip);
                return ip.getInteger();
            }
        };
    }
}
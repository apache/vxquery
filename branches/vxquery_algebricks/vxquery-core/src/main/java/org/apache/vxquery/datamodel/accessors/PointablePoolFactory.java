package org.apache.vxquery.datamodel.accessors;

import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;

import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class PointablePoolFactory {
    public static final PointablePoolFactory INSTANCE = new PointablePoolFactory();

    private PointablePoolFactory() {
    }

    public PointablePool createPointablePool() {
        PointablePool pp = new PointablePool();

        pp.register(TaggedValuePointable.class, TaggedValuePointable.FACTORY);
        pp.register(BooleanPointable.class, BooleanPointable.FACTORY);
        pp.register(BytePointable.class, BytePointable.FACTORY);
        pp.register(ShortPointable.class, ShortPointable.FACTORY);
        pp.register(IntegerPointable.class, IntegerPointable.FACTORY);
        pp.register(LongPointable.class, LongPointable.FACTORY);
        pp.register(FloatPointable.class, FloatPointable.FACTORY);
        pp.register(DoublePointable.class, DoublePointable.FACTORY);
        pp.register(UTF8StringPointable.class, UTF8StringPointable.FACTORY);
        pp.register(SequencePointable.class, SequencePointable.FACTORY);
        pp.register(VoidPointable.class, VoidPointable.FACTORY);
        pp.register(CodedQNamePointable.class, CodedQNamePointable.FACTORY);

        pp.register(NodeTreePointable.class, NodeTreePointable.FACTORY);
        pp.register(DocumentNodePointable.class, DocumentNodePointable.FACTORY);
        pp.register(ElementNodePointable.class, ElementNodePointable.FACTORY);
        pp.register(AttributeNodePointable.class, AttributeNodePointable.FACTORY);
        pp.register(TextOrCommentNodePointable.class, TextOrCommentNodePointable.FACTORY);
        pp.register(PINodePointable.class, PINodePointable.FACTORY);

        return pp;
    }
}
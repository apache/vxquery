package org.apache.vxquery.runtime.functions.index;

import java.io.DataInputStream;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;

public class IndexConstructorScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public IndexConstructorScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final UTF8StringPointable collectionP = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final TaggedValuePointable nodep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final ByteBufferInputStream bbis = new ByteBufferInputStream();
        final DataInputStream di = new DataInputStream(bbis);
        final SequenceBuilder sb = new SequenceBuilder();
        final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();
        final int partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        final String nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            private boolean first = true;

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                IndexConstructorUtil.evaluate(args, result, collectionP, bbis, di, sb, abvs, nodeIdProvider,
                        abvsFileNode, nodep, first, true, nodeId);
            }

        };
    }
}

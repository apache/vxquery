package org.apache.vxquery.runtime.functions.step;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class ChildPathStepScalarEvaluator extends AbstractPathStepScalarEvaluator {
    private final TaggedValuePointable rootTVP;

    private final DocumentNodePointable dnp;

    private final ElementNodePointable enp;

    public ChildPathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args, ctx);
        rootTVP = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
        enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    }

    @Override
    protected void getSequence(NodeTreePointable ntp, SequencePointable seqp) throws SystemException {
        ntp.getRootNode(rootTVP);
        switch (rootTVP.getTag()) {
            case ValueTag.DOCUMENT_NODE_TAG:
                rootTVP.getValue(dnp);
                dnp.getContent(ntp, seqp);
                return;

            case ValueTag.ELEMENT_NODE_TAG:
                rootTVP.getValue(enp);
                if (enp.childrenChunkExists()) {
                    enp.getChildrenSequence(ntp, seqp);
                    return;
                }
        }
        XDMConstants.setEmptySequence(seqp);
    }
}
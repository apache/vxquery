package org.apache.vxquery.runtime.functions.node;

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.builders.nodes.AttributeNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class AttributeNodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final AttributeNodeBuilder anb;

    private final XSQNamePointable namep;

    private final UTF8StringPointable strp;

    public AttributeNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        anb = new AttributeNodeBuilder();
        namep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
        strp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        anb.reset(mvs);
        TaggedValuePointable nameArg = args[0];
        if (nameArg.getTag() != ValueTag.XS_QNAME_TAG) {
            throw new SystemException(ErrorCode.XPST0081);
        }
        nameArg.getValue(namep);
        namep.getUri(strp);
        int uriCode = db.lookup(strp);
        namep.getPrefix(strp);
        int prefixCode = db.lookup(strp);
        namep.getLocalName(strp);
        int localCode = db.lookup(strp);
        anb.setName(uriCode, localCode, prefixCode);
        TaggedValuePointable valueArg = args[1];
        anb.setValue(valueArg);
        anb.finish();
    }

    @Override
    protected boolean createsDictionary() {
        return true;
    }
}
package org.apache.vxquery.runtime.functions.node;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractNodeConstructorScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final IHyracksTaskContext ctx;

    private final ArrayBackedValueStorage abvs;

    private final DictionaryBuilder db;

    private final ArrayBackedValueStorage contentAbvs;

    public AbstractNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        abvs = new ArrayBackedValueStorage();
        db = createsDictionary() ? new DictionaryBuilder() : null;
        contentAbvs = createsDictionary() ? new ArrayBackedValueStorage() : abvs;
    }

    @Override
    protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        abvs.reset();
        contentAbvs.reset();
        try {
            DataOutput mainOut = abvs.getDataOutput();
            mainOut.write(ValueTag.NODE_TREE_TAG);
            byte header = (byte) (createsDictionary() ? NodeTreePointable.HEADER_DICTIONARY_EXISTS_MASK : 0);
            mainOut.write(header);
            constructNode(db, args, contentAbvs);
            if (createsDictionary()) {
                db.write(abvs);
                abvs.append(contentAbvs);
            }
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    protected abstract void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException;

    protected abstract boolean createsDictionary();
}
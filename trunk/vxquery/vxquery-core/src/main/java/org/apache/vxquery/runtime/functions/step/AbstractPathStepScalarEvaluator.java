package org.apache.vxquery.runtime.functions.step;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractPathStepScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final DynamicContext dCtx;

    private final IntegerPointable ip;

    private final NodeTreePointable ntp;

    private final SequencePointable seqp;

    private final ArrayBackedValueStorage seqAbvs;

    private final SequenceBuilder seqb;

    private final ArrayBackedValueStorage nodeAbvs;

    private final TaggedValuePointable itemTvp;

    private INodeFilter filter;

    private boolean first;

    public AbstractPathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args);
        dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        seqAbvs = new ArrayBackedValueStorage();
        seqb = new SequenceBuilder();
        nodeAbvs = new ArrayBackedValueStorage();
        itemTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        first = true;
    }

    private void setNodeTest(SequenceType sType) {
        final NodeType nodeType = (NodeType) sType.getItemType();
        switch (nodeType.getNodeKind()) {
            case ANY:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return true;
                    }
                };
                break;

            case ATTRIBUTE: {
                AttributeType aType = (AttributeType) nodeType;
                NameTest nameTest = aType.getNameTest();
                byte[] uri = nameTest.getUri();
                byte[] localName = nameTest.getLocalName();
                final UTF8StringPointable urip = (UTF8StringPointable) (uri == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                final UTF8StringPointable localp = (UTF8StringPointable) (localName == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                if (uri != null) {
                    urip.set(uri, 0, uri.length);
                }
                if (localName != null) {
                    localp.set(localName, 0, localName.length);
                }
                final IPointable temp = VoidPointable.FACTORY.createPointable();
                final AttributeNodePointable anp = (AttributeNodePointable) AttributeNodePointable.FACTORY
                        .createPointable();
                final CodedQNamePointable cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY.createPointable();
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        if (tvp.getTag() != ValueTag.ATTRIBUTE_NODE_TAG) {
                            return false;
                        }
                        tvp.getValue(anp);
                        anp.getName(cqp);
                        if (urip != null) {
                            ntp.getString(cqp.getNamespaceCode(), temp);
                            if (urip.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        if (localp != null) {
                            ntp.getString(cqp.getLocalCode(), temp);
                            if (localp.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                break;
            }

            case COMMENT:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.COMMENT_NODE_TAG;
                    }
                };
                break;

            case DOCUMENT:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.DOCUMENT_NODE_TAG;
                    }
                };
                break;

            case ELEMENT: {
                ElementType eType = (ElementType) nodeType;
                NameTest nameTest = eType.getNameTest();
                byte[] uri = nameTest.getUri();
                byte[] localName = nameTest.getLocalName();
                final UTF8StringPointable urip = (UTF8StringPointable) (uri == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                final UTF8StringPointable localp = (UTF8StringPointable) (localName == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                if (uri != null) {
                    urip.set(uri, 0, uri.length);
                }
                if (localName != null) {
                    localp.set(localName, 0, localName.length);
                }
                final IPointable temp = VoidPointable.FACTORY.createPointable();
                final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
                final CodedQNamePointable cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY.createPointable();
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        if (tvp.getTag() != ValueTag.ELEMENT_NODE_TAG) {
                            return false;
                        }
                        tvp.getValue(enp);
                        enp.getName(cqp);
                        if (urip != null) {
                            ntp.getString(cqp.getNamespaceCode(), temp);
                            if (urip.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        if (localp != null) {
                            ntp.getString(cqp.getLocalCode(), temp);
                            if (localp.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                break;
            }

            case PI:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.PI_NODE_TAG;
                    }
                };
                break;

            case TEXT:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.TEXT_NODE_TAG;
                    }
                };
                break;
        }
    }

    protected abstract void getSequence(NodeTreePointable ntp, SequencePointable seqp) throws SystemException;

    @Override
    protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        try {
            if (first) {
                if (args[1].getTag() != ValueTag.XS_INT_TAG) {
                    throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
                }
                args[1].getValue(ip);
                int typeCode = ip.getInteger();
                SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
                setNodeTest(sType);
                first = false;
            }
            if (args[0].getTag() != ValueTag.NODE_TREE_TAG) {
                throw new SystemException(ErrorCode.SYSE0001);
            }
            args[0].getValue(ntp);
            getSequence(ntp, seqp);
            seqAbvs.reset();
            seqb.reset(seqAbvs);
            int seqSize = seqp.getEntryCount();
            for (int i = 0; i < seqSize; ++i) {
                seqp.getEntry(i, itemTvp);
                if (matches()) {
                    appendNodeToResult();
                }
            }
            seqb.finish();
            result.set(seqAbvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    private boolean matches() {
        return filter.accept(ntp, itemTvp);
    }

    private void appendNodeToResult() throws IOException {
        nodeAbvs.reset();
        DataOutput mainOut = nodeAbvs.getDataOutput();
        mainOut.write(ValueTag.NODE_TREE_TAG);
        boolean hasDictionary = ntp.dictionaryExists() && hasDictionary(itemTvp.getTag());
        byte header = (byte) (hasDictionary ? NodeTreePointable.HEADER_DICTIONARY_EXISTS_MASK : 0);
        mainOut.write(header);
        if (hasDictionary) {
            mainOut.write(ntp.getByteArray(), ntp.getDictionaryOffset(), ntp.getDictionarySize());
        }
        mainOut.write(itemTvp.getByteArray(), itemTvp.getStartOffset(), itemTvp.getLength());
        seqb.addItem(nodeAbvs);
    }

    private boolean hasDictionary(byte tag) {
        switch (tag) {
            case ValueTag.ATTRIBUTE_NODE_TAG:
            case ValueTag.DOCUMENT_NODE_TAG:
            case ValueTag.ELEMENT_NODE_TAG:
                return true;
        }
        return false;
    }

    private interface INodeFilter {
        public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp);
    }
}
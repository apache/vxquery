package org.apache.vxquery.runtime.functions.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.AttributeNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.builders.nodes.ElementNodeBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class ElementNodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final ElementNodeBuilder enb;

    private final AttributeNodeBuilder anb;

    private final List<ElementNodeBuilder> freeENBList;

    private final XSQNamePointable namep;

    private final CodedQNamePointable cqp;

    private final UTF8StringPointable strp;

    private final SequencePointable seqp;

    private final PointablePool ppool;

    public ElementNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        enb = new ElementNodeBuilder();
        anb = new AttributeNodeBuilder();
        freeENBList = new ArrayList<ElementNodeBuilder>();
        namep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
        cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY.createPointable();
        strp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        ppool = PointablePoolFactory.INSTANCE.createPointablePool();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        enb.reset(mvs);
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
        enb.setName(uriCode, localCode, prefixCode);
        TaggedValuePointable valueArg = args[1];
        enb.startAttributeChunk();
        int index = processAttributes(valueArg, db);
        enb.endAttributeChunk();
        enb.startChildrenChunk();
        if (index >= 0) {
            processChildren(valueArg, index, db);
        }
        enb.endChildrenChunk();
        enb.finish();
    }

    private int processAttributes(TaggedValuePointable tvp, DictionaryBuilder db) throws IOException {
        if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
            tvp.getValue(seqp);
            TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
            for (int i = 0; i < seqp.getEntryCount(); ++i) {
                seqp.getEntry(i, tempTvp);
                if (!processIfAttribute(tempTvp, db)) {
                    return i;
                }
            }
            ppool.giveBack(tempTvp);
        } else {
            if (!processIfAttribute(tvp, db)) {
                return 0;
            }
        }
        return -1;
    }

    private boolean processIfAttribute(TaggedValuePointable tvp, DictionaryBuilder db) throws IOException {
        if (tvp.getTag() != ValueTag.NODE_TREE_TAG) {
            return false;
        }
        NodeTreePointable ntp = ppool.takeOne(NodeTreePointable.class);
        try {
            tvp.getValue(ntp);
            TaggedValuePointable innerTvp = ppool.takeOne(TaggedValuePointable.class);
            try {
                ntp.getRootNode(innerTvp);
                if (innerTvp.getTag() != ValueTag.ATTRIBUTE_NODE_TAG) {
                    return false;
                }
                AttributeNodePointable anp = ppool.takeOne(AttributeNodePointable.class);
                try {
                    innerTvp.getValue(anp);
                    copyAttribute(enb, db, ntp, anp);
                } finally {
                    ppool.giveBack(anp);
                }
                return true;
            } finally {
                ppool.giveBack(innerTvp);
            }
        } finally {
            ppool.giveBack(ntp);
        }
    }

    private void copyAttribute(ElementNodeBuilder enb, DictionaryBuilder db, NodeTreePointable ntp,
            AttributeNodePointable anp) throws IOException {
        UTF8StringPointable strp = ppool.takeOne(UTF8StringPointable.class);
        VoidPointable vp = ppool.takeOne(VoidPointable.class);
        try {
            enb.startAttribute(anb);
            anp.getName(cqp);
            int newURICode = recode(cqp.getNamespaceCode(), ntp, db, strp);
            int newPrefixCode = recode(cqp.getPrefixCode(), ntp, db, strp);
            int newLocalCode = recode(cqp.getLocalCode(), ntp, db, strp);
            anb.setName(newURICode, newLocalCode, newPrefixCode);
            anp.getValue(ntp, vp);
            anb.setValue(vp);
            enb.endAttribute(anb);
        } finally {
            ppool.giveBack(vp);
            ppool.giveBack(strp);
        }
    }

    private void copyElement(ElementNodeBuilder enb, DictionaryBuilder db, NodeTreePointable ntp,
            ElementNodePointable enp) throws IOException {
        UTF8StringPointable strp = ppool.takeOne(UTF8StringPointable.class);
        SequencePointable seqp = ppool.takeOne(SequencePointable.class);
        AttributeNodePointable anp = ppool.takeOne(AttributeNodePointable.class);
        TaggedValuePointable tvp = ppool.takeOne(TaggedValuePointable.class);
        ElementNodePointable cenp = ppool.takeOne(ElementNodePointable.class);
        try {
            ElementNodeBuilder tempEnb = createENB();
            enb.startChild(tempEnb);
            enp.getName(cqp);
            int newURICode = recode(cqp.getNamespaceCode(), ntp, db, strp);
            int newPrefixCode = recode(cqp.getPrefixCode(), ntp, db, strp);
            int newLocalCode = recode(cqp.getLocalCode(), ntp, db, strp);
            tempEnb.setName(newURICode, newLocalCode, newPrefixCode);
            tempEnb.startAttributeChunk();
            if (enp.attributesChunkExists()) {
                enp.getAttributeSequence(ntp, seqp);
                for (int i = 0; i < seqp.getEntryCount(); ++i) {
                    seqp.getEntry(i, tvp);
                    tvp.getValue(anp);
                    copyAttribute(tempEnb, db, ntp, anp);
                }
            }
            tempEnb.endAttributeChunk();
            tempEnb.startChildrenChunk();
            if (enp.childrenChunkExists()) {
                enp.getChildrenSequence(ntp, seqp);
                for (int i = 0; i < seqp.getEntryCount(); ++i) {
                    seqp.getEntry(i, tvp);
                    byte nTag = tvp.getTag();
                    switch (nTag) {
                        case ValueTag.ELEMENT_NODE_TAG: {
                            tvp.getValue(cenp);
                            copyElement(tempEnb, db, ntp, cenp);
                            break;
                        }
                        case ValueTag.COMMENT_NODE_TAG:
                        case ValueTag.PI_NODE_TAG:
                        case ValueTag.TEXT_NODE_TAG: {
                            tempEnb.addChild(tvp);
                            break;
                        }
                    }
                }
            }
            tempEnb.endChildrenChunk();
            enb.endChild(tempEnb);
            freeENB(tempEnb);
        } finally {
            ppool.giveBack(cenp);
            ppool.giveBack(tvp);
            ppool.giveBack(anp);
            ppool.giveBack(seqp);
            ppool.giveBack(strp);
        }
    }

    private void copyDocument(ElementNodeBuilder enb, DictionaryBuilder db, NodeTreePointable ntp,
            DocumentNodePointable dnp) throws IOException {
        SequencePointable seqp = ppool.takeOne(SequencePointable.class);
        AttributeNodePointable anp = ppool.takeOne(AttributeNodePointable.class);
        TaggedValuePointable tvp = ppool.takeOne(TaggedValuePointable.class);
        ElementNodePointable cenp = ppool.takeOne(ElementNodePointable.class);
        try {
            dnp.getContent(ntp, seqp);
            for (int i = 0; i < seqp.getEntryCount(); ++i) {
                seqp.getEntry(i, tvp);
                if (tvp.getTag() == ValueTag.ELEMENT_NODE_TAG) {
                    tvp.getValue(cenp);
                    copyElement(enb, db, ntp, cenp);
                    break;
                }
            }
        } finally {
            ppool.giveBack(cenp);
            ppool.giveBack(tvp);
            ppool.giveBack(anp);
            ppool.giveBack(seqp);
        }

    }

    private int recode(int oldCode, NodeTreePointable ntp, DictionaryBuilder db, UTF8StringPointable tempStrp) {
        ntp.getString(oldCode, tempStrp);
        return db.lookup(tempStrp);
    }

    private void processChildren(TaggedValuePointable tvp, int start, DictionaryBuilder db) throws IOException,
            SystemException {
        if (tvp.getTag() == ValueTag.SEQUENCE_TAG) {
            tvp.getValue(seqp);
            TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
            for (int i = start; i < seqp.getEntryCount(); ++i) {
                seqp.getEntry(i, tempTvp);
                processChild(tempTvp, db);
            }
            ppool.giveBack(tempTvp);
        } else {
            processChild(tvp, db);
        }
    }

    private void processChild(TaggedValuePointable tvp, DictionaryBuilder db) throws IOException, SystemException {
        if (tvp.getTag() != ValueTag.NODE_TREE_TAG) {
            enb.addChild(tvp);
        } else {
            NodeTreePointable ntp = ppool.takeOne(NodeTreePointable.class);
            try {
                tvp.getValue(ntp);
                TaggedValuePointable innerTvp = ppool.takeOne(TaggedValuePointable.class);
                try {
                    ntp.getRootNode(innerTvp);
                    byte nTag = innerTvp.getTag();
                    switch (nTag) {
                        case ValueTag.ATTRIBUTE_NODE_TAG: {
                            throw new SystemException(ErrorCode.XQTY0024);
                        }
                        case ValueTag.ELEMENT_NODE_TAG: {
                            ElementNodePointable enp = ppool.takeOne(ElementNodePointable.class);
                            try {
                                innerTvp.getValue(enp);
                                copyElement(enb, db, ntp, enp);
                            } finally {
                                ppool.giveBack(enp);
                            }
                            break;
                        }
                        case ValueTag.COMMENT_NODE_TAG:
                        case ValueTag.PI_NODE_TAG:
                        case ValueTag.TEXT_NODE_TAG: {
                            enb.addChild(innerTvp);
                            break;
                        }
                        case ValueTag.DOCUMENT_NODE_TAG: {
                            DocumentNodePointable dnp = ppool.takeOne(DocumentNodePointable.class);
                            try {
                                innerTvp.getValue(dnp);
                                copyDocument(enb, db, ntp, dnp);
                            } finally {
                                ppool.giveBack(dnp);
                            }
                            break;
                        }
                    }
                } finally {
                    ppool.giveBack(innerTvp);
                }
            } finally {
                ppool.giveBack(ntp);
            }
        }
    }

    private ElementNodeBuilder createENB() {
        if (freeENBList.isEmpty()) {
            return new ElementNodeBuilder();
        }
        return freeENBList.remove(freeENBList.size() - 1);
    }

    private void freeENB(ElementNodeBuilder enb) {
        freeENBList.add(enb);
    }

    @Override
    protected boolean createsDictionary() {
        return true;
    }
}
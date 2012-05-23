package org.apache.vxquery.datamodel;

import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/*
 * Element {
 *  ElementHeader header;
 *  NamePtr namePtr;
 *  NamePtr typePtr?;
 *  LocalNodeId nodeId?;
 *  NamespaceChunk nsChunk?;
 *  Sequence attrChunk?;
 *  Sequence childrenChunk?;
 * }
 * 
 * ElementHeader (padded) {
 *  bit nsChunkExists;
 *  bit attrChunkExists;
 *  bit childrenChunkExists;
 * }
 * 
 * NamePtr {
 *  int[3] stringPtrs;
 * }
 * 
 * LocalNodeId {
 *  int32 id;
 * }
 * 
 * NamespaceChunk {
 *  byte[size] chunkSizeInBytes; // size = decode(ElementHeader.nsChunkSizeSize)
 *  NamePtr[2][chunkSizeInBytes / (sizeof(NamePtr) * 2)] namespaces;
 * }
 */
public class ElementNodePointable extends AbstractPointable {
    private static final byte NS_CHUNK_EXISTS_MASK = (0x1 << 0);
    private static final byte ATTRIBUTES_CHUNK_EXISTS_MASK = (0x1 << 1);
    private static final byte CHILDREN_CHUNK_EXISTS_MASK = (0x1 << 2);

    private static final int HEADER_SIZE = 1;
    private static final int LOCAL_NODE_ID_SIZE = 4;
    private static final int NS_ENTRY_SIZE = 4 * 2;
    private static final int NS_CHUNK_SIZE_SIZE = 4;

    public boolean nsChunkExists() {
        return (getHeader() & NS_CHUNK_EXISTS_MASK) != 0;
    }

    public boolean attributesChunkExists() {
        return (getHeader() & ATTRIBUTES_CHUNK_EXISTS_MASK) != 0;
    }

    public boolean childrenChunkExists() {
        return (getHeader() & CHILDREN_CHUNK_EXISTS_MASK) != 0;
    }

    public void getName(QNamePointable name) {
        name.set(bytes, getNameOffset(), getNameSize());
    }

    public void getTypeName(NodeTreePointable nodeTree, QNamePointable typeName) {
        if (nodeTree.typeExists()) {
            typeName.set(bytes, getTypeOffset(), getTypeSize(nodeTree));
        } else {
            typeName.set(null, -1, -1);
        }
    }

    public int getLocalNodeId(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? IntegerPointable.getInteger(bytes, getLocalNodeIdOffset(nodeTree)) : -1;
    }

    public int getNamespaceEntryCount(NodeTreePointable nodeTree) {
        return nsChunkExists() ? IntegerPointable.getInteger(bytes, getNamespaceChunkOffset(nodeTree)) : 0;
    }

    public int getNamespacePrefixCode(NodeTreePointable nodeTree, int nsEntryIdx) {
        if (!nsChunkExists()) {
            return -1;
        }
        if (getNamespaceEntryCount(nodeTree) <= nsEntryIdx) {
            throw new IndexOutOfBoundsException(nsEntryIdx + " >= " + getNamespaceEntryCount(nodeTree));
        }
        return IntegerPointable.getInteger(bytes, getNamespaceChunkOffset(nodeTree) + NS_CHUNK_SIZE_SIZE
                + NS_ENTRY_SIZE * nsEntryIdx);
    }

    public int getNamespaceURICode(NodeTreePointable nodeTree, int nsEntryIdx) {
        if (!nsChunkExists()) {
            return -1;
        }
        if (getNamespaceEntryCount(nodeTree) <= nsEntryIdx) {
            throw new IndexOutOfBoundsException(nsEntryIdx + " >= " + getNamespaceEntryCount(nodeTree));
        }
        return IntegerPointable.getInteger(bytes, getNamespaceChunkOffset(nodeTree) + NS_CHUNK_SIZE_SIZE
                + NS_ENTRY_SIZE * nsEntryIdx + 1);
    }

    public void getAttributeSequence(NodeTreePointable nodeTree, SequencePointable attributes) {
        if (attributesChunkExists()) {
            attributes.set(bytes, getAttributeChunkOffset(nodeTree), getAttributeChunkSize(nodeTree));
        } else {
            attributes.set(null, -1, -1);
        }
    }

    public void getChildrenSequence(NodeTreePointable nodeTree, SequencePointable children) {
        if (childrenChunkExists()) {
            children.set(bytes, getChildrenChunkOffset(nodeTree), getChildrenChunkSize(nodeTree));
        } else {
            children.set(null, -1, -1);
        }
    }

    private byte getHeader() {
        return BytePointable.getByte(bytes, start);
    }

    private int getNameOffset() {
        return start + HEADER_SIZE;
    }

    private int getNameSize() {
        return QNamePointable.SIZE;
    }

    private int getTypeOffset() {
        return getNameOffset() + getNameSize();
    }

    private int getTypeSize(NodeTreePointable nodeTree) {
        return nodeTree.typeExists() ? QNamePointable.SIZE : 0;
    }

    private int getLocalNodeIdOffset(NodeTreePointable nodeTree) {
        return getTypeOffset() + getTypeSize(nodeTree);
    }

    private int getLocalNodeIdSize(NodeTreePointable nodeTree) {
        return nodeTree.nodeIdExists() ? LOCAL_NODE_ID_SIZE : 0;
    }

    private int getNamespaceChunkOffset(NodeTreePointable nodeTree) {
        return getLocalNodeIdOffset(nodeTree) + getLocalNodeIdSize(nodeTree);
    }

    private int getNamespaceChunkSize(NodeTreePointable nodeTree) {
        return nsChunkExists() ? getNamespaceEntryCount(nodeTree) * NS_ENTRY_SIZE + NS_CHUNK_SIZE_SIZE : 0;
    }

    private int getAttributeChunkOffset(NodeTreePointable nodeTree) {
        return getNamespaceChunkOffset(nodeTree) + getNamespaceChunkSize(nodeTree);
    }

    private int getAttributeChunkSize(NodeTreePointable nodeTree) {
        return attributesChunkExists() ? SequencePointable.getSequenceLength(bytes, getAttributeChunkOffset(nodeTree))
                : 0;
    }

    private int getChildrenChunkOffset(NodeTreePointable nodeTree) {
        return getAttributeChunkOffset(nodeTree) + getAttributeChunkSize(nodeTree);
    }

    private int getChildrenChunkSize(NodeTreePointable nodeTree) {
        return childrenChunkExists() ? SequencePointable.getSequenceLength(bytes, getChildrenChunkOffset(nodeTree)) : 0;
    }
}
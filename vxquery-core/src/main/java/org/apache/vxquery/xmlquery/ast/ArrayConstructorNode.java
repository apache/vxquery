package org.apache.vxquery.xmlquery.ast;

import java.util.List;

import org.apache.vxquery.util.SourceLocation;

public class ArrayConstructorNode extends ASTNode {
    private List<ASTNode> items;

    public ArrayConstructorNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.ARRAY_CONSTRUCTOR;
    }

    public List<ASTNode> getItems() {
        return items;
    }

    public void setItems(List<ASTNode> items) {
        this.items = items;
    }

}

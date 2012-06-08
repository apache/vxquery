package org.apache.vxquery.datamodel.builders.base;

import org.apache.vxquery.datamodel.builders.AttributeNodeBuilder;
import org.apache.vxquery.datamodel.builders.CommentNodeBuilder;
import org.apache.vxquery.datamodel.builders.DictionaryBuilder;
import org.apache.vxquery.datamodel.builders.ElementNodeBuilder;
import org.apache.vxquery.datamodel.builders.PINodeBuilder;
import org.apache.vxquery.datamodel.builders.TextNodeBuilder;

public interface IDMBuilderPool {
    public DictionaryBuilder getDictionaryBuilder();

    public void returnDictionaryBuilder(DictionaryBuilder db);

    public ElementNodeBuilder getElementNodeBuilder();

    public void returnElementNodeBuilder(ElementNodeBuilder enb);

    public AttributeNodeBuilder getAttributeNodeBuilder();

    public void returnAttributeNodeBuilder(AttributeNodeBuilder anb);

    public CommentNodeBuilder getCommentNodeBuilder();

    public void returnCommentNodeBuilder(CommentNodeBuilder cnb);

    public TextNodeBuilder getTextNodeBuilder();

    public void returnTextNodeBuilder(TextNodeBuilder tnb);

    public PINodeBuilder getPINodeBuilder();

    public void returnPINodeBuilder(PINodeBuilder pnb);
}
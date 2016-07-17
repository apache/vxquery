package org.apache.vxquery.runtime.functions.index.updateIndex;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Class for storing metadata information for vxquery index.
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@XmlRootElement(name = "indexes")
public class VXQueryIndex {

    private List<XmlMetadataCollection> indexes;

    public List<XmlMetadataCollection> getIndex() {
        return indexes;
    }

    @XmlElement(name = "index", type = XmlMetadataCollection.class)
    public void setIndex(List<XmlMetadataCollection> index) {
        this.indexes = index;
    }
}

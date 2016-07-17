package org.apache.vxquery.runtime.functions.index.updateIndex;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Class for holding the collection information and the list of XML metadata related to the xml files in the
 * collection.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "index")
public class XmlMetadataCollection {

    @XmlAttribute(name = "location")
    private String indexLocation;

    @XmlAttribute(name = "collection")
    private String collection;

    @XmlElement(name = "file", type = XmlMetadata.class)
    private List<XmlMetadata> metadataList;

    public List<XmlMetadata> getMetadataList() {
        return metadataList;
    }

    public void setMetadataList(List<XmlMetadata> metadataList) {
        this.metadataList = metadataList;
    }

    public String getIndexLocation() {
        return indexLocation;
    }

    public void setIndexLocation(String indexLocation) {
        this.indexLocation = indexLocation;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }
}

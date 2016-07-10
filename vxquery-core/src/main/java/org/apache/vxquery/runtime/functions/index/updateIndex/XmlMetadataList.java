package org.apache.vxquery.runtime.functions.index.updateIndex;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Class for holding the list of XML metadata objects.
 * This class will be written to an xml file.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "indexes")
public class XmlMetadataList {

    @XmlElement(name = "index", type = XmlMetadata.class)
    private List<XmlMetadata> metadataList;

    public List<XmlMetadata> getMetadataList() {
        return metadataList;
    }

    public void setMetadataList(List<XmlMetadata> metadataList) {
        this.metadataList = metadataList;
    }
}

/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.runtime.functions.index.update;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
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

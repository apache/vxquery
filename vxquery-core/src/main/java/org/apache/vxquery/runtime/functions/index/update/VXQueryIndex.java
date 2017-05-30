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

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
package org.apache.vxquery.xmlquery.ast;

import java.util.List;

import org.apache.vxquery.util.SourceLocation;

public class ModuleImportNode extends ASTNode {
    private String prefix;
    private String targetNS;
    private List<String> locations;

    public ModuleImportNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.MODULE_IMPORT;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getTargetNS() {
        return targetNS;
    }

    public void setTargetNS(String targetNS) {
        this.targetNS = targetNS;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }
}

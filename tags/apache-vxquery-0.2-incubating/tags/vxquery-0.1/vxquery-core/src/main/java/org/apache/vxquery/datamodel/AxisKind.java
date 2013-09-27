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
package org.apache.vxquery.datamodel;

public enum AxisKind {
    ANCESTOR(false),
    ANCESTOR_OR_SELF(false),
    ATTRIBUTE(true),
    CHILD(true),
    DESCENDANT(true),
    DESCENDANT_OR_SELF(true),
    FOLLOWING(true),
    FOLLOWING_SIBLING(true),
    PARENT(false),
    PRECEDING(false),
    PRECEDING_SIBLING(false),
    SELF(true);

    private boolean fwdAxis;

    private AxisKind(boolean fwdAxis) {
        this.fwdAxis = fwdAxis;
    }

    public boolean isForwardAxis() {
        return fwdAxis;
    }
}
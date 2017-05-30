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
package org.apache.vxquery.common;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.vxquery.functions.BuiltinFunctions;

import java.util.HashSet;
import java.util.Set;

public class VXQueryCommons {

    public static final Set<FunctionIdentifier> collectionFunctions = new HashSet<>();

    static {
        collectionFunctions.add(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier());
        collectionFunctions.add(BuiltinFunctions.FN_COLLECTION_WITH_TAG_2.getFunctionIdentifier());
    }

    public static final Set<FunctionIdentifier> indexingFunctions = new HashSet<>();

    static {
        indexingFunctions.add(BuiltinFunctions.FN_BUILD_INDEX_ON_COLLECTION_1.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_COLLECTION_FROM_INDEX_1.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_DELETE_INDEX_1.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_UPDATE_INDEX_1.getFunctionIdentifier());
    }

}

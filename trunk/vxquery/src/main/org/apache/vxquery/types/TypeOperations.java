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
package org.apache.vxquery.types;

import java.util.Arrays;

public class TypeOperations {
    public static Quantifier quantifier(XQType type) {
        return Quantifier.QUANT_STAR;
    }

    public static XQType primeType(XQType type) {
        return AnyItemType.INSTANCE;
    }

    public static XQType quantified(XQType t, Quantifier q) {
        return Quantifier.QUANT_ONE.equals(q) ? t : new QuantifiedType(t, q);
    }

    public static XQType union(XQType... types) {
        return new ComposedType(Arrays.asList(types), Composer.UNION);
    }

    public static XQType sequence(XQType... types) {
        return new ComposedType(Arrays.asList(types), Composer.SEQUENCE);
    }

    public static XQType shuffle(XQType... types) {
        return new ComposedType(Arrays.asList(types), Composer.SHUFFLE);
    }

    public static XQType intersect(XQType t1, XQType t2) {
        return AnyItemType.INSTANCE;
    }
}
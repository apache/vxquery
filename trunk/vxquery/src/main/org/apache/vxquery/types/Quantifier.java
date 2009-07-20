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

public enum Quantifier {
    QUANT_ZERO, QUANT_ONE, QUANT_QUESTION, QUANT_STAR, QUANT_PLUS;

    private static final Quantifier[][] QPRODUCT = { { QUANT_ZERO, QUANT_ZERO, QUANT_ZERO, QUANT_ZERO, QUANT_ZERO },
            { QUANT_ZERO, QUANT_ONE, QUANT_QUESTION, QUANT_STAR, QUANT_PLUS },
            { QUANT_ZERO, QUANT_QUESTION, QUANT_QUESTION, QUANT_STAR, QUANT_STAR },
            { QUANT_ZERO, QUANT_STAR, QUANT_STAR, QUANT_STAR, QUANT_STAR },
            { QUANT_ZERO, QUANT_PLUS, QUANT_STAR, QUANT_STAR, QUANT_PLUS } };

    public static Quantifier product(Quantifier quant1, Quantifier quant2) {
        return QPRODUCT[quant1.ordinal()][quant2.ordinal()];
    }

    public boolean allowsEmptySequence() {
        return this == QUANT_QUESTION || this == QUANT_STAR || this == QUANT_ZERO;
    }

    public boolean allowsRepeating() {
        return this == QUANT_PLUS || this == QUANT_STAR;
    }
}
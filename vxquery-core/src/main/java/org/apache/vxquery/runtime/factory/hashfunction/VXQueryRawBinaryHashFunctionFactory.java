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
package org.apache.vxquery.runtime.factory.hashfunction;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;

public class VXQueryRawBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {
    private static final long serialVersionUID = 1L;

    public static IBinaryHashFunctionFactory INSTANCE = new VXQueryRawBinaryHashFunctionFactory();

    private VXQueryRawBinaryHashFunctionFactory() {

    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {

        return MurmurHash3BinaryHashFunctionFamily.INSTANCE.createBinaryHashFunction(0);

    }

}

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
package org.apache.vxquery.exceptions;

import org.apache.vxquery.util.SourceLocation;

public final class DefaultSystemExceptionFactory implements SystemExceptionFactory {
    public static final SystemExceptionFactory INSTANCE = new DefaultSystemExceptionFactory();

    private DefaultSystemExceptionFactory() {
    }

    @Override
    public SystemException createException() {
        return new SystemException(ErrorCode.SYSE0001);
    }

    @Override
    public SystemException createException(ErrorCode code) {
        return new SystemException(code);
    }

    @Override
    public SystemException createException(ErrorCode code, SourceLocation location) {
        return new SystemException(code, location);
    }
}
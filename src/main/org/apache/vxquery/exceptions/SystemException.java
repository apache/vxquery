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

public class SystemException extends Exception {
    private static final long serialVersionUID = 1L;

    private ErrorCode code;

    public SystemException(ErrorCode code) {
        super(code + ": " + code.getDescription());
        this.code = code;
    }

    public SystemException(ErrorCode code, Throwable cause) {
        super(code + ": " + code.getDescription(), cause);
    }

    public SystemException(ErrorCode code, SourceLocation loc) {
        super(code + ": " + loc + code.getDescription());
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
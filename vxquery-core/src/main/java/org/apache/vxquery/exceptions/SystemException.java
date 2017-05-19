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

import java.text.MessageFormat;
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.vxquery.util.SourceLocation;

public class SystemException extends HyracksDataException {
    private static final long serialVersionUID = 1L;

    private final ErrorCode code;

    public SystemException(ErrorCode code) {
        super(message(code, null));
        this.code = code;
    }

    public SystemException(ErrorCode code, Object... params) {
        super(message(code, null, params));
        this.code = code;
    }

    public SystemException(ErrorCode code, Throwable cause) {
        super(message(code, null), cause);
        this.code = code;
    }

    public SystemException(ErrorCode code, Throwable cause, Object... params) {
        super(message(code, null, params), cause);
        this.code = code;
    }

    public SystemException(ErrorCode code, SourceLocation loc) {
        super(message(code, loc));
        this.code = code;
    }
    
    public SystemException(ErrorCode code, SourceLocation loc, Throwable cause) {
        super(message(code, loc), cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }

    private static String message(ErrorCode code, SourceLocation loc) {
        String description = code.getDescription();
        return code + ": " + (loc == null ? "" : loc + " ") + (description != null ? description : "");
    }

    private static String message(ErrorCode code, SourceLocation loc, Object... params) {
        String description = code.getDescription();
        return code + ": " + (loc == null ? "" : loc + " ")
                + (description != null ? MessageFormat.format(description, params) : Arrays.deepToString(params));
    }

}

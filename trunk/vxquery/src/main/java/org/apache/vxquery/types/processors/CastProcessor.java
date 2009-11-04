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
package org.apache.vxquery.types.processors;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.exceptions.SystemExceptionFactory;
import org.apache.vxquery.types.XQType;

/**
 * Interface used to perform value-casting and ask casting related questions
 * regarding a target type.
 */
public interface CastProcessor {
    /**
     * Performs casting of the given value to be an instance of the target type.
     * 
     * @param avf
     *            Atomic Value Factory
     * @param value
     *            The value to be cast to the target type.
     * @return - Value whose type is a sub-type of the target type.
     * @throws SystemException
     *             if a casting error occurs.
     */
    public XDMValue cast(AtomicValueFactory avf, XDMValue value, SystemExceptionFactory ieFactory,
            StaticContext ctx) throws SystemException;

    /**
     * Decides if the given value can be cast to the target type.
     * 
     * @param value
     *            The value to be tested for castability.
     * @return <code>true</code> if the given value is castable to the target
     *         type, <code>false</code> otherwise.
     */
    public boolean castable(XDMValue value, StaticContext ctx);

    /**
     * Decides if any instance of the given type is castable to the target type.
     * 
     * @param type
     *            Input type.
     * @return <code>Boolean.TRUE</code> if the casting is always possible,
     *         <code>Boolean.FALSE</code> if the casting is never possible,
     *         <code>null</code> if casting is sometimes possible.
     */
    public Boolean castable(XQType type);

    /**
     * Returns the error code of the error when casting from an instance of the
     * given type to the target type. This call is useful only when
     * castable(XQType) returns Boolean.FALSE
     * 
     * @param type
     *            The input type
     * @return Error code if it can be determined from the type, else returns
     *         <code>null</code>
     */
    public ErrorCode getCastFailureErrorCode(XQType type);
}
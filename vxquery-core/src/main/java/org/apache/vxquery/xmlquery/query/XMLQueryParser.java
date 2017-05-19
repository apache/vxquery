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
package org.apache.vxquery.xmlquery.query;

import java.io.Reader;
import java.util.List;

import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.SourceLocation;
import org.apache.vxquery.xmlquery.ast.ModuleNode;

public class XMLQueryParser {
    public static ModuleNode parse(String sourceName, Reader input) throws SystemException {
        XMLQuery parser = new XMLQuery(input);
        parser.setSourceName(sourceName);
        try {
            return parser.CUnit();
        } catch (TokenMgrError tme) {
            throw new SystemException(ErrorCode.XPST0003, tme);
        } catch (ParseException pe) {
            List<SystemException> exceptions = parser.getExceptions();
            if (!exceptions.isEmpty()) {
                throw exceptions.get(0);
            }
            SourceLocation loc = new SourceLocation(sourceName, pe.currentToken.beginLine, pe.currentToken.beginColumn);
            throw new SystemException(ErrorCode.XPST0003, loc, pe);
        }
    }

    private XMLQueryParser() {
    }
}

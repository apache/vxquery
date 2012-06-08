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
package org.apache.vxquery.xmlparser;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.XDMItem;

public interface IEventAcceptor {
    public void open() throws SystemException;

    public void startDocument() throws SystemException;

    public void endDocument() throws SystemException;

    public void startElement(String uri, String localName, String prefix) throws SystemException;

    public void endElement() throws SystemException;

    public void namespace(String prefix, String uri) throws SystemException;

    public void attribute(String uri, String localName, String prefix, String stringValue) throws SystemException;

    public void text(char[] chars, int start, int length) throws SystemException;

    public void comment(String content) throws SystemException;

    public void pi(String target, String content) throws SystemException;

    public void item(XDMItem item) throws SystemException;

    public void close();
}
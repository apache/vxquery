/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.xtest.util.tests;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class BufferedParsedCharacterStream extends AbstractDiskTest {
    @Override
    public String getMessage() {
        return "Buffered Parsed - Character Stream";
    }

    @Override
    public long test(String filename, int bufferSize) throws IOException {
        try {
            if (bufferSize > 0) {
                parser.setProperty("http://apache.org/xml/properties/input-buffer-size", new Integer(bufferSize));
            }
            InputStreamReader f = new InputStreamReader(new FileInputStream(filename));
            InputSource in = new InputSource();
            in.setCharacterStream(f);
            parser.parse(in);
        } catch (SAXException e) {
            e.printStackTrace();
        }
        return -1;
    }
}

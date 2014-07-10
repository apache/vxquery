package org.apache.vxquery.xtest.util.tests;

import org.xml.sax.XMLReader;

public interface IDiskTest {
    public void setBufferSize(int bufferSize);

    public void setFilename(String absolutePath);

    public void setParser(XMLReader parser);

    public void run();

    public void start();
}

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
package org.apache.vxquery.xtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class to sort the final test results.
 */
public class ResultFileSorter {

    private final String path;
    private static final Logger LOGGER = Logger.getLogger(ResultFileSorter.class.getName());

    public ResultFileSorter(String path) throws FileNotFoundException {
        this.path = path;
    }

    /**
     * The method to sort the test case results.
     */
    public void sortFile() {
        //TODO : Optimize for large files.
        File resultFile = new File(path);
        try {
            FileReader fileReader = new FileReader(resultFile);
            BufferedReader reader = new BufferedReader(fileReader);
            String line;
            ArrayList<String> fullText = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                fullText.add(line);
            }
            reader.close();
            fileReader.close();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.log(Level.INFO, "Sorting.....");
            }
            Collections.sort(fullText, String.CASE_INSENSITIVE_ORDER);
            String[] sortedText = fullText.toArray(new String[fullText.size()]);
            eraseFile(resultFile);
            writeToFile(sortedText);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Trouble with sorting the file: " + resultFile.getAbsolutePath(), e);
        }
    }

    /**
     * Method to delete existing the file.
     *
     * @throws FileNotFoundException
     */
    private boolean eraseFile(File file) throws FileNotFoundException {
        return file.delete();
    }

    /**
     * Method to write the sorted content to the new file.
     *
     * @param text
     *            : The sorted array of test case results.
     * @throws FileNotFoundException
     */
    private void writeToFile(String[] text) throws FileNotFoundException {
        File newFile = new File(path);
        PrintWriter writer = new PrintWriter(newFile);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.log(Level.INFO, "Writing to file started.");
        }
        for (String s : text) {
            writer.write(s + "\n");
        }
        writer.close();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.log(Level.INFO, "Writing to file finished successfully.");
        }

    }
}

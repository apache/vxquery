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
package org.apache.vxquery.hdfs2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSFunctions {
	
    private Configuration conf;
    private FileSystem fs;
    private String conf_path;
    private final String conf_folder = "/etc/hadoop/";
    
    /**
     * Create the configuration and add the paths for core-site and hdfs-site as resources.
     * Initialize an instance a hdfs FileSystem for this configuration.
     * @param hadoop_conf_filepath 
     */
    public HDFSFunctions()
    {
    	if(locateConf())
    	{
	        this.conf = new Configuration();
	        conf.addResource(new Path(this.conf_path + "core-site.xml"));
	        conf.addResource(new Path(this.conf_path + "hdfs-site.xml"));
	        try {
	            fs =  FileSystem.get(conf);
	        } catch (IOException ex) {
	            System.err.println(ex);
	        }
    	}
    	else
    	{
    		//print error cannot locate configuration folder for HDFS
    	}
    }
    
    /**
     * Returns true if the file path exists or it is located somewhere in the home directory of the user that called the function.
     * Searches in subdirectories of the home directory too.
     * @param filename
     * @return 
     */
    public boolean isLocatedInHDFS(String filename)
    {
    	 try {
             //search file path
             if (fs.exists(new Path(filename)))
             {
                 return true;
             }
         } catch (IOException ex) {
             System.err.println(ex);
         }
        //Search every file and folder in the home directory
        if (searchInDirectory(fs.getHomeDirectory(), filename) != null)
        {
            return true;
        }
        return false;
    }
    
    /**
     * Searches the given directory and subdirectories for the file.
     * @param directory to search
     * @param filename of file we want
     * @return path if file exists in this directory.else return null.
     */
    public Path searchInDirectory(Path directory, String filename)
    {
        //Search every folder in the directory
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(directory, true);
            String[] parts;
            Path path;
            while(it.hasNext())
            {
                path = it.next().getPath();
                parts = path.toString().split("/");
                if(parts[parts.length-1].equals(filename))
                {
                    return path;
                }
            }
        } catch (IOException ex) {
            System.err.println(ex);
        }
        return null;
    }
    
    /**
     * Search in the system environment variables for the Hadoop/HDFS home folder to get the path to the configuration.
     * Variables it is checking are: HADOOP_HOME,HADOOP_PREFIX,HADOOP_CONF_DIR,HADOOP_HDFS_HOME.
     * @return true if is successfully finds the Hadoop/HDFS home directory
     */
    private boolean locateConf()
    {//HADOOP_HOME
    	String conf = System.getenv("HADOOP_HOME");
    	if (conf == null)
    	{//HADOOP_PREFIX
    		conf = System.getenv("HADOOP_PREFIX");
    		if (conf != null)
    		{
    			this.conf_path = conf + this.conf_folder;
    		}
    		else
    		{//HADOOP_CONF_DIR
    			conf = System.getenv("HADOOP_CONF_DIR");
    			if (conf != null)
    			{
    				this.conf_path = conf + this.conf_folder;
    			}
    			else
        		{//HADOOP_HDFS_HOME
        			conf = System.getenv("HADOOP_HDFS_HOME");
        			if (conf != null)
        			{
        				this.conf_path = conf + this.conf_folder;
        			}
        			else
        			{
        				return false;
        			}
        		}
    		}
    	}
    	else
    	{
    		this.conf_path = conf + this.conf_folder;;
    	}
    	return true;
    }
    
    /**
     * Upload a file/directory to HDFS.Filepath is the path in the local file system.dir is the destination path.
     * @param filepath
     * @param dir
     * @return
     */
    public boolean put(String filepath,String dir)
    {
    	if (this.fs != null)
    	{
    		Path path = new Path(filepath);
    		Path dest = new Path(dir);
    		try {
				if (fs.exists(dest))
				{
					fs.delete(dest, true); //recursive delete
					try {
						fs.copyFromLocalFile(path, dest);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	return false;
    }
    
    /**
     * Get instance of the hdfs file system if it is configured correctly.
     * Return null if there is no instance.
     * @return
     */
    public FileSystem getFileSystem()
    {
    	if (this.conf_path != null)
    	{
    		return this.fs;
    	}
    	else
    	{
    		return null;
    	}
    }
}

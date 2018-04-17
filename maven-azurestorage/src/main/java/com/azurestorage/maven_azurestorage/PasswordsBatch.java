package com.azurestorage.maven_azurestorage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PasswordsBatch {
    public static void main( String[] args )
    {
    		long startTime = System.nanoTime();
    		// Define the connection-string with your values.
		//final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=appserversdiag232;AccountKey=yXX7DE/aWLwQFTtx9Pc1PXRLs+iv2RSDT9lowMQqh9O2T8DwIaB1DRjuTeBp9x8uWPTx61sy5rMI+rWUyqj/rA==";
		
        
        try
        {            
         // The name of the file to open.
            String fileName = "C:\\Users\\pavan\\Downloads\\pwned-passwords-ordered-2.0.txt\\pwned-passwords-ordered-2.0.txt";

            // This will reference one line at a time
            String line = null;
            
            try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = 
                    new FileReader(fileName);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = 
                    new BufferedReader(fileReader); 
                int lines = 0;
                
				List<PasswordEntity> passwordList = new ArrayList<PasswordEntity>();
				
                while((line = bufferedReader.readLine()) != null) {
	                	
                		lines++;
                		
            			String[] split = line.split(":");
	                	
            			if(split == null || split.length != 2) {
	                		System.err.println("Invalid Line @ Line# -> "+ lines);
	                		continue;
	                	}
            			
            			passwordList.add(new PasswordEntity(split[0], split[1]));
            			
            			
            			if(passwordList.size() < 20000) {
            				continue;
            			} else {
            				new PasswordsBatchProcess(passwordList);
            				passwordList.clear();
            				continue;
            			}	
                }
                
                if(passwordList.size() > 0) {
                	new PasswordsBatchProcess(passwordList);
    			}
	            
	            System.err.println("Line ->" + lines);
                // Always close files.
                bufferedReader.close();         
            }
            catch(FileNotFoundException ex) {
                System.out.println(
                    "Unable to open file '" + 
                    fileName + "'");  
                ex.printStackTrace();
            }
            catch(IOException ex) {
                System.out.println(
                    "Error reading file '" 
                    + fileName + "'");
                ex.printStackTrace();
            }
        }
        catch (Exception e)
        {
            // Output the stack trace.
        		System.err.println("Failed Inserts");
            e.printStackTrace();
            
        }   
        
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println(totalTime);
    }
    
}

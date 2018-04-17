package com.azurestorage.maven_azurestorage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AppDropBoxSha2 {
    public static void main( String[] args )
    {
    		long startTime = System.nanoTime();
    		// Define the connection-string with your values.
		//final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=appserversdiag232;AccountKey=yXX7DE/aWLwQFTtx9Pc1PXRLs+iv2RSDT9lowMQqh9O2T8DwIaB1DRjuTeBp9x8uWPTx61sy5rMI+rWUyqj/rA==";
		
        
        try
        {            
         // The name of the file to open.
            String fileName = "C:\\Users\\pavan\\Downloads\\dropbox_08jUnk\\sha2.txt";

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
                
                // Create List with 10000 domain entries
				List<Breach> gm = new LinkedList<Breach>();
				List<Breach> yahooList = new LinkedList<Breach>();
				List<Breach> hotmailList = new LinkedList<Breach>();
				List<Breach> googleMailList = new LinkedList<Breach>();
				List<Breach> hotmailde = new LinkedList<Breach>();
				List<Breach> hotmailit = new LinkedList<Breach>();
				List<Breach> earthlink = new LinkedList<Breach>();
				List<Breach> live = new LinkedList<Breach>();
				List<Breach> yahoocoin = new LinkedList<Breach>();
				List<Breach> yahoocojp = new LinkedList<Breach>();
				List<Breach> comcastnet = new LinkedList<Breach>();
				List<Breach> hotmailfr = new LinkedList<Breach>();
				List<Breach> aol = new LinkedList<Breach>();
				
				
				List<Breach> otherList = new ArrayList<Breach>();
				
                while((line = bufferedReader.readLine()) != null) {
	                	
                		lines++;
                		
            			String[] split = line.split(":");
	                	
            			if(split == null || split.length != 2) {
	                		System.err.println("Invalid Line @ Line# -> "+ lines);
	                		continue;
	                	}
            			
            			if (line.contains("@hotmail.de")) {
            				
            				if(hotmailde.size() < 20000) {
                				hotmailde.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(hotmailde);
        					hotmailde.clear();
            				continue;
            			}
            			
            			if (line.contains("@hotmail.it")) {
            				
            				if(hotmailit.size() < 20000) {
            					hotmailit.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(hotmailit);
        					hotmailit.clear();
            				continue;
            			}
            			
            			if (line.contains("@aol.com:")) {
            				
            				if(aol.size() < 20000) {
            					aol.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(aol);
        					aol.clear();
            				continue;
            			}
            			
            			if (line.contains("@yahoo.co.in:")) {
            				
            				if(yahoocoin.size() < 20000) {
            					yahoocoin.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(yahoocoin);
        					yahoocoin.clear();
            				continue;
            			}
            			
            			if (line.contains("@comcast.net:")) {
            				
            				if(comcastnet.size() < 20000) {
            					comcastnet.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(comcastnet);
        					comcastnet.clear();
            				continue;
            			}
            			
            			if (line.contains("@earthlink.net:")) {
            				
            				if(earthlink.size() < 20000) {
            					earthlink.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(earthlink);
        					earthlink.clear();
            				continue;
            			}
            			
            			if (line.contains("@hotmail.fr:")) {
            				
            				if(hotmailfr.size() < 20000) {
            					hotmailfr.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(hotmailfr);
        					hotmailfr.clear();
            				continue;
            			}
            			
            			if (line.contains("@live.com:")) {
            				
            				if(live.size() < 20000) {
            					live.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(live);
        					live.clear();
            				continue;
            			}
            			
                	
            			if (line.contains("@gmail.com:")) {
            				
            				if(gm.size() < 20000) {
                				gm.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					new BatchProcess(gm);
            				gm.clear();
            				continue;
            			}
            			
            			if (line.contains("@googlemail.com:")) {
            				
            				if(googleMailList.size() < 20000) {
            					googleMailList.add(new Breach(split[0], split[1]));
                				continue;
            				}
        					
            				new BatchProcess(googleMailList);
            				googleMailList.clear();
            				continue;
            			}
            			
            			if (line.contains("@yahoo.com:")) {
            				
            				if(yahooList.size() < 20000) {
            					yahooList.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				new BatchProcess(yahooList);
            				yahooList.clear();
            				continue;
            			}
            			
            			if (line.contains("@hotmail.com:")) {
            				
            				if(hotmailList.size() < 20000) {
            					hotmailList.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				new BatchProcess(hotmailList);
        					hotmailList.clear();
            				continue;
            			}
            			
            			otherList.add(new Breach(split[0], split[1]));
            			if(otherList.size() < 200) {
             			continue;
            			} else {
            				ExecutorService service = Executors.newCachedThreadPool();
            				otherList.forEach((breach) -> {
            					service.execute(new IndividualBatch(breach));
            				});
        					service.shutdown();
        					try {
        						service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        					} catch (InterruptedException e) {
        					  System.err.println("IndividualBatch issue threads");
        					}
        					otherList.clear();
        					System.err.println("IndividualBatch Finally");
            			}
                }
                
                if(otherList.size() > 0) {
                	
	                	System.err.println("Mass Insertion Individual less than 25 & size is ->" + otherList.size());
	                	ExecutorService service = Executors.newCachedThreadPool();
	    				otherList.forEach((breach) -> {
	    					service.execute(new IndividualBatch(breach));
	    				});
						service.shutdown();
						try {
							service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
						  System.err.println("IndividualBatch issue threads");
						}
						
						System.err.println("IndividualBatch Finally");
                }
                
	            if(gm.size() > 0) {
	            	new BatchProcess(gm);
	            }
	            
	            if(googleMailList.size() > 0) {
	            	new BatchProcess(googleMailList);
	            }
	            
	            if(hotmailList.size() > 0) {
	            	new BatchProcess(hotmailList);
	            }
	            
	            if(yahooList.size() > 0) {
	            	new BatchProcess(yahooList);
	            }
                
	            if(hotmailde.size() > 0) {
	            	new BatchProcess(hotmailde);
	            }
	            
	            if(hotmailfr.size() > 0) {
	            	new BatchProcess(hotmailfr);
	            }
	            
	            if(hotmailit.size() > 0) {
	            	new BatchProcess(hotmailit);
	            }
	            
	            if(yahoocoin.size() > 0) {
	            	new BatchProcess(yahoocoin);
	            }
	            
	            if(yahoocojp.size() > 0) {
	            	new BatchProcess(yahoocojp);
	            }
	            
	            if(earthlink.size() > 0) {
	            	new BatchProcess(earthlink);
	            }
	            
	            if(aol.size() > 0) {
	            	new BatchProcess(aol);
	            }
	            
	            if(live.size() > 0) {
	            	new BatchProcess(live);
	            }
	            
	            if(comcastnet.size() > 0) {
	            	new BatchProcess(comcastnet);
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

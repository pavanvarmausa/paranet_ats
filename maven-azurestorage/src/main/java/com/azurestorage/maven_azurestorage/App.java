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

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.table.TableBatchOperation;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    		long startTime = System.nanoTime();
    		// Define the connection-string with your values.
		//final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=appserversdiag232;AccountKey=yXX7DE/aWLwQFTtx9Pc1PXRLs+iv2RSDT9lowMQqh9O2T8DwIaB1DRjuTeBp9x8uWPTx61sy5rMI+rWUyqj/rA==";
		
        
        try
        {            
         // The name of the file to open.
            String fileName = "/Users/pavankumarvarbhupatiraju/Desktop/dropbox_08jUnk/bf_3.txt";

            // This will reference one line at a time
            String line = null;
            
            try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = 
                    new FileReader(fileName);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = 
                    new BufferedReader(fileReader); 
                
//                List<Breach> gmailBucket = new ArrayList<Breach>();   
//                List<Breach> hotmailBucket = new ArrayList<Breach>();
//                List<Breach> yahooBucket = new ArrayList<Breach>();
                int lines = 0;
                // Create List with 10000 gmail emails
				List<Breach> gm = new LinkedList<Breach>();
				List<Breach> yahooList = new LinkedList<Breach>();
				List<Breach> hotmailList = new LinkedList<Breach>();
				List<Breach> googleMailList = new LinkedList<Breach>();
				
				
				List<Breach> otherList = new ArrayList<Breach>();
				
                while((line = bufferedReader.readLine()) != null) {
	                	
                		lines++;
                		
            			String[] split = line.split(":");
	                	
            			if(split == null || split.length != 2) {
	                		System.err.println("Invalid Line @ Line# -> "+ lines);
	                		continue;
	                	}
                	
            			if (line.contains("@gmail.com:")) {
            				//gmailBucket.add(new Breach(split[0], split[1]));
            				
            				if(gm.size() < 20000) {
                				gm.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				
            				//ExecutorService service = Executors.newCachedThreadPool();
            				
            				System.err.println("Hit 20000");
            				System.err.println("start->" + System.currentTimeMillis());
            				
            				//Multi start
        					
        					final List<List<Breach>> batch = Lists.partition(gm, 100);
        					ExecutorService service = Executors.newCachedThreadPool();
        					System.err.println("Batch Size--> " + batch.size());
        					for (List<Breach> list : batch) {
        					  // Add your code here
        						try {
        							System.err.println("Thread ->" + list.size());
        							service.execute(new GmailBatch(new TableBatchOperation(), list));
            					} catch (Exception e1) {
            						// TODO Auto-generated catch block
            						System.err.println("Gmail Entry issue @ Line - ");
            						e1.printStackTrace();
            					}
        					}
        					
        					System.err.println("before");
        					service.shutdown();
        					System.err.println("aftre");
        					try {
        						service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        					} catch (InterruptedException e) {
        					  System.err.println("issue threads");
        					}
        					
        					System.err.println("Finally");
        					
        					//Multi End
            				
            				System.err.println("end->" + System.currentTimeMillis());
            				gm.clear();
            				continue;
            			}
            			
            			if (line.contains("@googlemail.com:")) {
            				//gmailBucket.add(new Breach(split[0], split[1]));
            				
            				if(googleMailList.size() < 5000) {
            					googleMailList.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				
            				
            				
            				System.err.println("Hit 5000 googlemail");
            				
            				//Multi start
        					
        					final List<List<Breach>> batch = Lists.partition(googleMailList, 100);
        					ExecutorService service = Executors.newCachedThreadPool();
        					System.err.println("googlemail Batch Size--> " + batch.size());
        					for (List<Breach> list : batch) {
        					  // Add your code here
        						try {
        							System.err.println("googlemail Thread ->" + list.size());
        							service.execute(new GmailBatch(new TableBatchOperation(), list));
            					} catch (Exception e1) {
            						// TODO Auto-generated catch block
            						System.err.println("googlemail Entry issue @ Line - ");
            						e1.printStackTrace();
            					}
        					}
        					service.shutdown();
        					try {
        						service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        					} catch (InterruptedException e) {
        					  System.err.println("issue threads");
        					}
        					
        					System.err.println("Finally");
        					
        					//Multi End
            				googleMailList.clear();
            				continue;
            			}
            			
            			if (line.contains("@yahoo.com:")) {
            				//gmailBucket.add(new Breach(split[0], split[1]));
            				
            				if(yahooList.size() < 5000) {
            					yahooList.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				
            				
            				
            				System.err.println("Hit 5000 yahoo");
            				
            				//Multi start
        					
        					final List<List<Breach>> batch = Lists.partition(yahooList, 100);
        					ExecutorService service = Executors.newCachedThreadPool();
        					System.err.println("yahooList Batch Size--> " + batch.size());
        					for (List<Breach> list : batch) {
        					  // Add your code here
        						try {
        							System.err.println("yahooList Thread ->" + list.size());
        							service.execute(new GmailBatch(new TableBatchOperation(), list));
            					} catch (Exception e1) {
            						// TODO Auto-generated catch block
            						System.err.println("yahooList Entry issue @ Line - ");
            						e1.printStackTrace();
            					}
        					}
        					service.shutdown();
        					try {
        						service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        					} catch (InterruptedException e) {
        					  System.err.println("yahooList issue threads");
        					}
        					
        					System.err.println("yahooList Finally");
        					
        					//Multi End
            				yahooList.clear();
            				continue;
            			}
            			
            			if (line.contains("@hotmail.com:")) {
            				//gmailBucket.add(new Breach(split[0], split[1]));
            				
            				if(hotmailList.size() < 5000) {
            					hotmailList.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				
            				System.err.println("Hit 5000 hotmailList");
            				
            				//Multi start
        					
        					final List<List<Breach>> batch = Lists.partition(hotmailList, 100);
        					ExecutorService service = Executors.newCachedThreadPool();
        					System.err.println("hotmailList Batch Size--> " + batch.size());
        					for (List<Breach> list : batch) {
        					  // Add your code here
        						try {
        							System.err.println("hotmailList Thread ->" + list.size());
        							service.execute(new GmailBatch(new TableBatchOperation(), list));
            					} catch (Exception e1) {
            						// TODO Auto-generated catch block
            						System.err.println("hotmailList Entry issue @ Line - ");
            						e1.printStackTrace();
            					}
        					}
        					service.shutdown();
        					try {
        						service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        					} catch (InterruptedException e) {
        					  System.err.println("hotmailList issue threads");
        					}
        					
        					System.err.println("hotmailList Finally");
        					
        					//Multi End
        					hotmailList.clear();
            				continue;
            			}
            			
            			otherList.add(new Breach(split[0], split[1]));
            			if(otherList.size() < 100) {
             			continue;
            			} else {
            				System.err.println("Mass ****200*** Insertion Individual size-> " + otherList.size());
            				ExecutorService service = Executors.newCachedThreadPool();
            				otherList.forEach((breach) -> {
            					service.execute(new IndividualBatch(breach));
            				});
        					service.shutdown();
        					try {
        						service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
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
							service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
						} catch (InterruptedException e) {
						  System.err.println("IndividualBatch issue threads");
						}
						
						System.err.println("IndividualBatch Finally");
                }
                
	            if(gm.size() > 0) {
	            			final List<List<Breach>> batch = Lists.partition(gm, 100);
	            			ExecutorService service = Executors.newCachedThreadPool();
	            			System.err.println("Batch Size -> "+ batch.size());
	    					for (List<Breach> list : batch) {
	    						// Add your code here
	    						try {
	    							System.err.println("Thread ->" + list.size());
	    							service.execute(new GmailBatch(new TableBatchOperation(), list));
	        					} catch (Exception e1) {
	        						// TODO Auto-generated catch block
	        						System.err.println("Gmail Entry issue @ Line - ");
	        						e1.printStackTrace();
	        					}
	    					}
	    					
	    					System.err.println("small before");
	    					service.shutdown();
	    					System.err.println("small aftre");
	    					try {
	    						service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	    					} catch (InterruptedException e) {
	    					  System.err.println("small issue threads");
	    					}
	    					
	    					System.err.println("gmail endlist Finally");
	            }
	            
	            if(googleMailList.size() > 0) {
	        			final List<List<Breach>> batch = Lists.partition(googleMailList, 100);
	        			ExecutorService service = Executors.newCachedThreadPool();
	        			System.err.println("googleMailList Batch Size -> "+ batch.size());
						for (List<Breach> list : batch) {
							// Add your code here
							try {
								System.err.println("Thread ->" + list.size());
								service.execute(new GmailBatch(new TableBatchOperation(), list));
	    					} catch (Exception e1) {
	    						// TODO Auto-generated catch block
	    						System.err.println("googlemail Entry issue @ Line - ");
	    						e1.printStackTrace();
	    					}
						}
						
						service.shutdown();
						try {
							service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
						} catch (InterruptedException e) {
						  System.err.println("googlemaillist issue threads");
						}
						
						System.err.println("googlemaillist endlist Finally");
	            }
	            
	            if(hotmailList.size() > 0) {
	        			final List<List<Breach>> batch = Lists.partition(hotmailList, 100);
	        			ExecutorService service = Executors.newCachedThreadPool();
	        			System.err.println("hotmailList Batch Size -> "+ batch.size());
						for (List<Breach> list : batch) {
							// Add your code here
							try {
								System.err.println("Thread ->" + list.size());
								service.execute(new GmailBatch(new TableBatchOperation(), list));
	    					} catch (Exception e1) {
	    						// TODO Auto-generated catch block
	    						System.err.println("hotmailList Entry issue @ Line - ");
	    						e1.printStackTrace();
	    					}
						}
						
						service.shutdown();
						try {
							service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
						} catch (InterruptedException e) {
						  System.err.println("hotmailList issue threads");
						}
						
						System.err.println("hotmailList endlist Finally");
	            }
	            
	            if(yahooList.size() > 0) {
	        			final List<List<Breach>> batch = Lists.partition(yahooList, 100);
	        			ExecutorService service = Executors.newCachedThreadPool();
	        			System.err.println("yahooList Batch Size -> "+ batch.size());
						for (List<Breach> list : batch) {
							// Add your code here
							try {
								System.err.println("Thread ->" + list.size());
								service.execute(new GmailBatch(new TableBatchOperation(), list));
	    					} catch (Exception e1) {
	    						// TODO Auto-generated catch block
	    						System.err.println("yahooList Entry issue @ Line - ");
	    						e1.printStackTrace();
	    					}
						}
						
						service.shutdown();
						try {
							service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
						} catch (InterruptedException e) {
						  System.err.println("yahooList issue threads");
						}
						
						System.err.println("yahooList endlist Finally");
	            }
                
	            System.err.println("Line ->" + lines);
                // Always close files.
                bufferedReader.close();         
            }
            catch(FileNotFoundException ex) {
                System.out.println(
                    "Unable to open file '" + 
                    fileName + "'");                
            }
            catch(IOException ex) {
                System.out.println(
                    "Error reading file '" 
                    + fileName + "'");                  
                // Or we could just do this: 
                // ex.printStackTrace();
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

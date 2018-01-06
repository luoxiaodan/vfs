package socket;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import func.Slave;


import org.json.JSONArray;
import org.json.JSONObject;
import struct.VSFProtocols;

public class SlaveServer {
	private static ServerSocket server;
	
	public static String signRead="";
	private static long lastSendTime;
	private static Slave slave=new Slave();
	public static void main(String[] args) throws IOException, InterruptedException {    
		SlaveServer slaveServer=new SlaveServer();
		slave.IniSalve();
		slaveServer.Server();
		lastSendTime=System.currentTimeMillis();  
		
	    }
		
	    public  void Server() throws IOException{
	    	
	    	 try {    
	    		 	            
				ServerSocket serverSocket = new ServerSocket(Slave.SLAVE_PORT); 
				
				//=======================test===============
	            // new Thread(new KeepHeart()).start();//heartmessage
	/*			 slave.chunkOption("write", 1, 0, 2, "23");
	             slave.chunkOption("read", 1, 0, 2, "");
	             while(!signRead.equals("OK"));
	             if(signRead.equals("OK")){
	            	 byte[] content=slave.readChunk(1,0,2);
	            	 System.out.println(new String(content));   
	             }
	             */
	             while (true) {    
	                 
	                 Socket client = serverSocket.accept();    
	                  
	                 new HandlerThread(client);    
	             }    
	         } catch (Exception e) {    
	             System.out.println("server error: " + e.getMessage());    
	         }    
	    }
	   
	    class KeepHeart implements Runnable{  
	        long checkDelay = 10;  
	        long keepAliveDelay = 200;  
	        public KeepHeart(){}
	        public void run() {  
	            while(true){  
	                if(System.currentTimeMillis()-lastSendTime>keepAliveDelay){  
	                    try {  
	                       Socket heartsocket=new Socket(Slave.MASTER_IP,Slave.MASTER_PORT); 
	                       DataOutputStream out=new DataOutputStream(heartsocket.getOutputStream());
	                       out.write(Slave.HEARTMESSAGE.getBytes(),0,Slave.HEARTMESSAGE.getBytes().length);
	                       out.close();
	                    } catch (IOException e) {  
	                        e.printStackTrace();  
	                        
	                    }  
	                    lastSendTime = System.currentTimeMillis();  
	                }else{  
	                    try {  
	                        Thread.sleep(checkDelay);  
	                    } catch (InterruptedException e) {  
	                        e.printStackTrace();  
	                        
	                    }  
	                }  
	            }  
	        }
	    }
	    
	    private class HandlerThread implements Runnable {    
	        private Socket socket;    
	        public HandlerThread(Socket client) {    
	            socket = client;    
	            new Thread(this).start();    
	        }
			@Override
			public void run() {
				try {   
				   DataOutputStream out=new DataOutputStream(socket.getOutputStream());
	                   
	 		       int protocols=inputProtocols(socket);		
				  
				   if((protocols==VSFProtocols.WRITE_CHUNK)||(protocols==VSFProtocols.READ_CHUNK)){
					 
		        		switch (Integer.valueOf(protocols)) {
		                case VSFProtocols.WRITE_CHUNK:
		                    writeChunk(socket);		                		                     
		                    break;
	
		                case VSFProtocols.READ_CHUNK:
		                    readChunk(out);
		                    break;
		        		}
				   }else{
					   
					   if((protocols==VSFProtocols.INITIALIZE_CHUNK_INFO)||(protocols==VSFProtocols.NEW_CHUNK)||(protocols==VSFProtocols.RELEASE_CHUNK)){ //master   
						  
						   switch (protocols) {
			                case VSFProtocols.INITIALIZE_CHUNK_INFO://chunkinfo
			                    getChunkInfo(out);
			                    break;
	
			                case VSFProtocols.NEW_CHUNK://createchunk
			                    createChunk(out,socket);
			                    break;	
			                case VSFProtocols.RELEASE_CHUNK:
			                	deleteChunk(out);
			                	break;
			                }					   
						   
					   	
					   }
				   }
				 
					   
		
			// server.close();
			} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error : " + e.getMessage());
			}
			}
	
	public void writeChunk(Socket socket) throws IOException, InterruptedException{
		  
            
			  DataInputStream input = new DataInputStream(socket.getInputStream());
			  int chunkid = input.readInt();// chunk_id
			  int offset = input.readInt();// offset
			  int writelen = input.readInt();// writelen
        	  String contentBuff="";
       	 
        	  //content
        	  int contentCount = 0;
        	  byte[] tempBuf = new byte[Slave.UPLOAD_BUFFER_SIZE];
          	
          	  while(true){
          		if(contentCount >= writelen){
          			break;
          		}
          		int cRead = Math.min(writelen - contentCount, tempBuf.length);
          		int aRead = input.read(tempBuf, 0, cRead);          			
          			contentBuff+= new String(tempBuf);        		
          		    contentCount += aRead;
          	}
        	
        	  
           slave.chunkOption("write",chunkid,offset,writelen,contentBuff);
                           
           
            
	
	}
	
    public void readChunk(DataOutputStream out) throws NumberFormatException, IOException, InterruptedException{
	    
			DataInputStream input = new DataInputStream(socket.getInputStream());
			int chunkid = input.readInt();// chunk_id
			int offset = input.readInt();// offset
			int readlen = input.readInt();// readlen
						
			byte[] content = null;
			
		    signRead="";
		    
		    slave.chunkOption("read",chunkid,offset,readlen,"");
            while(!signRead.equals("OK"));
            if(signRead.equals("OK")){
            	content=slave.readChunk(chunkid,offset,readlen);
            }
		    
		    responesClient(out,"OK");

			
			int len = 0;
			for (int i = 0; i < content.length; ++i) {
				if (content[i] == '\0') {
					len = i;
					break;
				}
			}
		    out.writeInt(len);	
          
            out.writeInt(readlen);
	      
	    	int bufferSize = Slave.UPLOAD_BUFFER_SIZE;
			byte[] contentBuff = new byte[bufferSize];
			int contentCount = 0;
			while(contentCount < readlen){
				int writeNum = Math.min(bufferSize, readlen-contentCount);
				for(int i = 0; i < bufferSize; ++i){
					contentBuff[i] = content[contentCount+i];
				}
				out.write(contentBuff, 0, writeNum);
				out.flush();
				
				contentCount += writeNum;
			}               
	        
    }
    
    public void getChunkInfo(DataOutputStream out) throws IOException{
     	JSONArray chunkarray=slave.getChunkList();
    	String string=chunkarray.toString();
        
        byte[] bytes=string.getBytes();
       
        out.writeInt(bytes.length);
		out.write(bytes, 0, bytes.length);
       
    }
    
    public void deleteChunk(DataOutputStream out) throws NumberFormatException, Exception{
    	//doing
    	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    	boolean stateWruteChunk=slave.deleteChunk(Integer.valueOf(in.readLine()));
    	responseStatus(out,stateWruteChunk);
    }
    
    public void createChunk(DataOutputStream out,Socket socket) throws Exception{
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
    	byte[] bytes = new byte[length];
    	input.read(bytes, 0, length);
    	
        JSONObject chunk = new JSONObject(new String(bytes));
		boolean stateWruteChunk=slave.CreateChunk(chunk);
		responseStatus(out,stateWruteChunk);
        
    }
    
    public void responesClient(DataOutputStream out,String content) throws IOException{
    	   out.writeUTF(content);
		   System.out.println("response to client: " + content);
 	
    }
    
    public void responseStatus(DataOutputStream out,boolean check) throws IOException{
    	if(check){
      	  responesClient(out,VSFProtocols.MESSAGE_OK); 
           
        }else{
      	  responesClient(out,VSFProtocols.MASTER_REJECT); 
        }
    }
    public  int inputProtocols(Socket socket) throws IOException {		
    	DataInputStream input = new DataInputStream(socket.getInputStream());
		
    	byte[] protocolBuff = new byte[8];
		input.read(protocolBuff, 0, 8);
		int len = 0;
		for (int i = 0; i < protocolBuff.length; ++i) {
			if (protocolBuff[i] == '\0') {
				len = i;
				break;
			}
		}
		return Integer.valueOf(new String(protocolBuff, 0, len));
    	
    	
	}
  
    public  String inputString(Socket socket) throws IOException {
		DataInputStream input = new DataInputStream(socket.getInputStream());
		int length = input.readInt();

    	byte[] string = new byte[length];
		input.read(string, 0, length);
		int len = 0;
		for (int i = 0; i < string.length; ++i) {
			if (string[i] == '\0') {
				len = i;
				break;
			}
		}
	    return new String(string);
	    }
	    }
}

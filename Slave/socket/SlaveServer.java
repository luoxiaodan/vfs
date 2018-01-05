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
	
	/**
	* 初始化socket服务
	*/
	public static String signObj=null;
	public static String signWork="end";
	private static Slave slave=new Slave();
	public static void main(String[] args) throws IOException {    
		SlaveServer slaveServer=new SlaveServer();
		slave.IniSalve();
		PushBlockQueue.getInstance().start();
		slaveServer.initServer();
	}
	
	
	public void initServer() {
	try {
	// 创建一个ServerSocket在端口8080监听客户请求
	server = new ServerSocket(8888);
	createMessage();
	} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	}
	/**
	* 创建消息管理，一直接收消息
	*/
	private void createMessage() {
	try {
		System.out.println("waiting for client : ");
	
		Socket socket = server.accept();
		System.out.println("client socketPort : " + socket.getPort());
		
		new Thread(new Runnable() {
			public void run() {
			    createMessage();
		}
	}).start();
	
	BufferedReader bff = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
    	// Scanner scanner = new Scanner(socket.getInputStream());
		String line = "";
		// accept this socket message
		while (true) {
			//Thread.sleep(500);
			
			while ((line = bff.readLine()) != null) {		
			  
			   if(line.equals(String.valueOf(VSFProtocols.WRITE_CHUNK))||line.equals(String.valueOf(VSFProtocols.READ_CHUNK))){
				   System.out.println("put");
				   PushBlockQueue.getInstance().put(line+":"+socket.getPort());
			   }else{
				   
				   if(line.equals(VSFProtocols.INITIALIZE_CHUNK_INFO)||line.equals(VSFProtocols.NEW_CHUNK)||line.equals(VSFProtocols.RELEASE_CHUNK)){ //master   
					  
					   switch (Integer.valueOf(line)) {
		                case VSFProtocols.INITIALIZE_CHUNK_INFO://chunkinfo
		                    getChunkInfo(socket);
		                    break;

		                case VSFProtocols.NEW_CHUNK://createchunk
		                    createChunk(socket);
		                    break;	
		                case VSFProtocols.RELEASE_CHUNK:
		                	deleteChunk(socket);
		                	break;
		                }					   
					   
				   }else{
				   
					   responesClient(socket,"Please wait...");
				   }
			   }
			   Thread.sleep(500);
			   System.out.println("context signObj :"+line+" "+signObj);
			   if(signObj.equals(line+":"+socket.getPort())){
				  
				    
	        		String[] Info=signObj.split(":");
	        		responesClient(socket,Info[0]);
	        		switch (Integer.valueOf(Info[0])) {
	                case VSFProtocols.WRITE_CHUNK:
	                    writeChunk(socket);
	                    break;

	                case VSFProtocols.READ_CHUNK:
	                    readChunk(socket);
	                    break;
	                
	                }
				   
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
	
	public void writeChunk(Socket socket) throws IOException{
		  
            
              DataInputStream input = new DataInputStream(socket.getInputStream());  
      	      int chunkid = inputInt(socket);//chunk_id        	  
        	  int offset= inputInt(socket);//offset       	  
        	  int writelen = inputInt(socket);//writelen
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
        	
        	  
              boolean stateWruteChunk=slave.writeChunk(chunkid,offset,writelen,contentBuff);
              signWork="end";              
              responseStatus(socket,stateWruteChunk);
            
	
	}

    public void readChunk(Socket socket) throws NumberFormatException, IOException{
	        DataOutputStream out = new DataOutputStream(socket.getOutputStream());				    
	        int chunkid = inputInt(socket);//chunk_id        	  
      	    int offset= inputInt(socket);//offset       	  
      	    int readlen = inputInt(socket);//readlen
		    byte[] content=slave.readChunk(chunkid,offset,readlen);		    				
		    responesClient(socket,"OK");
          
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
	        out.close(); 
    }
    
    public void getChunkInfo(Socket socket) throws IOException{
    	DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    	JSONArray chunkarray=slave.getChunkList();
    	String string=chunkarray.toString();
        
        byte[] bytes=string.getBytes();
       
        out.writeInt(bytes.length);
		out.write(bytes, 0, bytes.length);
        out.close();
    }
    
    public void deleteChunk(Socket socket) throws NumberFormatException, Exception{
    	//doing
    	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    	boolean stateWruteChunk=slave.deleteChunk(Integer.valueOf(in.readLine()));
    	responseStatus(socket,stateWruteChunk);
    }
    
    public void createChunk(Socket socket) throws Exception{
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());    
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
    	byte[] bytes = new byte[length];
    	input.read(bytes, 0, length);
    	
        JSONObject chunk = new JSONObject(new String(bytes));
		boolean stateWruteChunk=slave.CreateChunk(chunk);
		responseStatus(socket,stateWruteChunk);
        
    }
    
    public void responesClient(Socket socket,String content) throws IOException{
    	   DataOutputStream out = new DataOutputStream(socket.getOutputStream());    
    	   out.writeUTF(content);
		   System.out.println("response to client: " + content);
 		   out.close();
    }
    
    public void responseStatus(Socket socket,boolean check) throws IOException{
    	if(check){
      	  responesClient(socket,VSFProtocols.MESSAGE_OK); 
           
        }else{
      	  responesClient(socket,VSFProtocols.MASTER_REJECT); 
        }
    }
    public  int inputInt(Socket socket) throws IOException {		
    	return Integer.valueOf(inputString(socket));
	}
    public  String inputString(Socket socket) throws IOException {
		DataInputStream input = new DataInputStream(socket.getInputStream());
		int length = input.readInt();
    	byte[] bytes = new byte[length];
    	input.read(bytes, 0,length);
    	return new String(bytes);
	}

    
}

package vfs.func;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import vfs.socket.SlaveServer;
import vfs.struct.VSFProtocols;


public class Chunk {
	public int chunkId;
	public String status;
	public boolean isRent=false;
//	public PushBlockQueue queue;
	public  List<Integer> copyids; 
	public  List<Integer> offset;
	public  List<Integer> len;
	public  List<String> content;
	public  List<String> option;
	public static final String WRITE="write";
	public static final String READ="read";
	public  Slave slave;
	public  boolean close=false;
	public HandlerThread handlerThread;
	private static long lastSendTime;
	public Chunk(int _chunkid,Slave _slave){
		chunkId=_chunkid;
		status="idle";		
		copyids=new ArrayList<Integer>();
		offset=new ArrayList<Integer>();
		len=new ArrayList<Integer>();
		content=new ArrayList<String>();
		option=new ArrayList<String>();
		slave=_slave;
		//queue.getInstance().start();
		lastSendTime=System.currentTimeMillis();  
		handlerThread=new HandlerThread(_slave);  
	}
	
	public void WRchunk(String _option,int _offset,int _len,String _content) throws InterruptedException{
		//queue.getInstance().start();
	       option.add(_option);
		if(_option.equals(WRITE)){
			offset.add(_offset);
			len.add(_len);
			content.add(_content);
		
		}
		//queue.getInstance().put(option);		
	}
	
	  private class HandlerThread implements Runnable {    
	        private Slave slave;
	        long checkDelay = 10;  
	        long keepAliveDelay = 200;  
	       
	        public HandlerThread(Slave _slave) {    
	        	slave = _slave;    	        	                   
	            new Thread(this).start();  
	            
	        }
			@Override
			public void run() {
				//System.out.println(status);
				while(true){
					 if(System.currentTimeMillis()-lastSendTime>keepAliveDelay){ 
						 if(isRent){
							 isRent=false;
							 try {
								if(SocketUtil.sendToMaster(VSFProtocols.RENT_CHUN)){
									 isRent=true;
								 }
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						 }
					 }
					
						
					if(slave.chunkRent.get(0).option.size()>0){
							
					if(status.equals("idle")){
								System.out.println("size :"+Chunk.this.option.size());
						status="comp";
						String _option=option.get(0);
						switch(_option){
						case WRITE:
							slave.writeChunk(chunkId,offset.get(0),len.get(0),content.get(0));
							for(int i=0;i<copyids.size();i++){
								slave.writeChunk(copyids.get(i),offset.get(0),len.get(0),content.get(0));							
							}
							offset.remove(0);
							len.remove(0);
							content.remove(0);
							option.remove(0);
							status="idle";
							break;
						case READ:
							status="comp";
							SlaveServer.signRead="OK";
							
							break;
						}
						}
				}
				}
			}
	  }
	
}

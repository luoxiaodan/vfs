package vfs.func;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import vfs.socket.SlaveServer;
import vfs.struct.ChunkInfo;
import vfs.struct.VSFProtocols;

public class Chunk {
	public int chunkId;
	public String status;
	public boolean isRent = false;
	// public PushBlockQueue queue;
	public List<ChunkInfo> copyids;
	public List<Integer> offset;
	public List<Integer> len;
	public List<String> content;
	public List<String> option;
	public static final String WRITE = "write";
	public static final String READ = "read";
	public Slave slave;
	public boolean close = false;
	public HandlerThread handlerThread;
	private static long lastSendTime;
	private List<DataOutputStream> out;

	public Chunk(int _chunkid, Slave _slave) {
		chunkId = _chunkid;
		status = "idle";
		copyids = new ArrayList<ChunkInfo>();
		offset = new ArrayList<Integer>();
		len = new ArrayList<Integer>();
		content = new ArrayList<String>();
		option = new ArrayList<String>();
		slave = _slave;
		// queue.getInstance().start();
		lastSendTime = System.currentTimeMillis();
		handlerThread = new HandlerThread(_slave);
		out=new ArrayList<DataOutputStream>();
	}
	
	

	public void WRchunk(String _option, int _offset, int _len, String _content,DataOutputStream _out) throws InterruptedException {
		// queue.getInstance().start();
		
		System.out.println("chunk put data");
		if (_option.equals(WRITE)) {
			offset.add(_offset);
			len.add(_len);
			content.add(_content);
			out.add(_out);
		}
		option.add(_option);
		// queue.getInstance().put(option);
       while(!status.equals("idle"));
       if (status.equals("idle")) {
		if (option.size() > 0) {
			System.out.println("option size > 0 status: "+status);
			
				status = "comp";
				String current_option = option.get(0);
				switch (current_option) {
				case WRITE:
					slave.writeChunk(chunkId, offset.get(0), len.get(0), content.get(0));
					for (int i = 0; i < copyids.size(); i++) {
						ChunkInfo copy = copyids.get(i);
						try {
							SocketUtil.writeCopyChunk(copy.slaveIP, copy.port, copy.chunkId, offset.get(0),
									len.get(0), content.get(0));
							
											
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						DataOutputStream output=out.get(0);
						try {
							SocketUtil.responesClient(output, VSFProtocols.MESSAGE_OK);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
										
					out.remove(0);
					offset.remove(0);
					len.remove(0);
					content.remove(0);
					option.remove(0);
					status = "idle";
					SlaveServer.signWrite = "OK";
					break;
				case READ:
					status = "read";
					

					break;
				}
			}
		}
	}

	private class HandlerThread implements Runnable {
		private Slave slave;
		long checkDelay = 100000;
		long keepAliveDelay = 20000000;

		public HandlerThread(Slave _slave) {
			slave = _slave;
			new Thread(this).start();

		}

		@Override
		public void run() {
			// System.out.println(status);
			while (!close) {
				//System.out.println("option running : "+option.size());
				if (System.currentTimeMillis() - lastSendTime > keepAliveDelay) {
					if (isRent) {
						isRent = false;
						try {
							if (SocketUtil.sendToMaster(VSFProtocols.RENEW_LEASE, chunkId)) {
								isRent = true;
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						lastSendTime = System.currentTimeMillis();
					}
				}

			}
		}
	}

}

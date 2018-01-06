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
import vfs.struct.VSFProtocols;

public class SlaveServer {
	private static ServerSocket server;

	/**
	 * ��ʼ��socket����
	 */
	public static String signObj = null;
	public static String signWork = "end";
	private static Slave slave = new Slave();

	public static void main(String[] args) throws IOException {
		SlaveServer slaveServer = new SlaveServer();
		slave.IniSalve();
		PushBlockQueue.getInstance().start();
		slaveServer.Server();
	}

	public void Server() throws IOException {
		slave.IniSalve();
		try {
			ServerSocket serverSocket = new ServerSocket(Slave.SLAVE_PORT);
			while (true) {

				Socket client = serverSocket.accept();

				new HandlerThread(client);
			}
		} catch (Exception e) {
			System.out.println("server error: " + e.getMessage());
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
			System.out.println("start running");
			try {

				int protocols = inputProtocols(socket);
				System.out.println("protocol id" + protocols);

				if ((protocols == VSFProtocols.WRITE_CHUNK) || (protocols == VSFProtocols.READ_CHUNK)) {
					switch (protocols) {
					case VSFProtocols.WRITE_CHUNK:
						writeChunk(socket);
						break;

					case VSFProtocols.READ_CHUNK:
						readChunk(socket);
						break;

					}

				} else {

					if ((protocols == VSFProtocols.INITIALIZE_CHUNK_INFO) || (protocols == VSFProtocols.NEW_CHUNK)
							|| (protocols == VSFProtocols.RELEASE_CHUNK)) { // master

						switch (protocols) {
						case VSFProtocols.INITIALIZE_CHUNK_INFO:// chunkinfo
							getChunkInfo(socket);
							break;

						case VSFProtocols.NEW_CHUNK:// createchunk
							createChunk(socket);
							break;
						case VSFProtocols.RELEASE_CHUNK:
							deleteChunk(socket);
							break;
						}
						// PushBlockQueue.getInstance().put(String.valueOf(protocols)
						// + ":" + socket.getPort());

					} else {

						responesClient(socket, "Please wait...");
					}
				}
				Thread.sleep(500);

				// server.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("error : " + e.getMessage());
			}
		}

		public void writeChunk(Socket socket) throws IOException {

			DataInputStream input = new DataInputStream(socket.getInputStream());
//			int chunkid = inputInt(socket);// chunk_id
//			int offset = inputInt(socket);// offset
//			int writelen = inputInt(socket);// writelen
			// modified by zsy
			int chunkid = input.readInt();// chunk_id
			int offset = input.readInt();// offset
			int writelen = input.readInt();// writelen
			
			String contentBuff = "";

			// content
			int contentCount = 0;
			byte[] tempBuf = new byte[Slave.UPLOAD_BUFFER_SIZE];

			while (true) {
				if (contentCount >= writelen) {
					break;
				}
				int cRead = Math.min(writelen - contentCount, tempBuf.length);
				int aRead = input.read(tempBuf, 0, cRead);
				contentBuff += new String(tempBuf);
				contentCount += aRead;
			}

			boolean stateWruteChunk = slave.writeChunk(chunkid, offset, writelen, contentBuff);
			signWork = "end";
			responseStatus(socket, stateWruteChunk);

		}

		public void readChunk(Socket socket) throws NumberFormatException, IOException {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
//			int chunkid = inputInt(socket);// chunk_id
//			int offset = inputInt(socket);// offset
//			int readlen = inputInt(socket);// readlen
			// modified by zsy
			DataInputStream input = new DataInputStream(socket.getInputStream());
			int chunkid = input.readInt();// chunk_id
			int offset = input.readInt();// offset
			int readlen = input.readInt();// readlen
						
			byte[] content = slave.readChunk(chunkid, offset, readlen);
//			responesClient(socket, "OK");
			responesClientWithStream(out, "OK");
			
			out.writeInt(readlen);

			int bufferSize = Slave.UPLOAD_BUFFER_SIZE;
			byte[] contentBuff = new byte[bufferSize];
			int contentCount = 0;
			while (contentCount < readlen) {
				int writeNum = Math.min(bufferSize, readlen - contentCount);
				for (int i = 0; i < bufferSize; ++i) {
					contentBuff[i] = content[contentCount + i];
				}
				out.write(contentBuff, 0, writeNum);
				out.flush();

				contentCount += writeNum;
			}
			out.close();
		}

		public void getChunkInfo(Socket socket) throws IOException {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			JSONArray chunkarray = slave.getChunkList();
			String string = chunkarray.toString();

			byte[] bytes = string.getBytes();

			out.writeInt(bytes.length);
			out.write(bytes, 0, bytes.length);
			out.close();
		}

		public void deleteChunk(Socket socket) throws NumberFormatException, Exception {
			// doing
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			boolean stateWruteChunk = slave.deleteChunk(Integer.valueOf(in.readLine()));
			responseStatus(socket, stateWruteChunk);
		}

		public void createChunk(Socket socket) throws Exception {
			DataInputStream input = new DataInputStream(socket.getInputStream());
			int length = input.readInt();
			byte[] bytes = new byte[length];
			input.read(bytes, 0, length);

			JSONObject chunk = new JSONObject(new String(bytes));
			boolean stateWruteChunk = slave.CreateChunk(chunk);
			responseStatus(socket, stateWruteChunk);

		}

		public void responesClient(Socket socket, String content) throws IOException {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			out.writeUTF(content);
			System.out.println("response to client: " + content);
			out.close();
		}
		
		public void responesClientWithStream(DataOutputStream out, String content) throws IOException {
			out.writeUTF(content);
			System.out.println("response to client: " + content);
		}

		public void responseStatus(Socket socket, boolean check) throws IOException {
			if (check) {
				responesClient(socket, VSFProtocols.MESSAGE_OK);

			} else {
				responesClient(socket, VSFProtocols.MASTER_REJECT);
			}
		}

		public int inputProtocols(Socket socket) throws IOException {
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

		public int inputInt(Socket socket) throws IOException {
			return Integer.valueOf(inputString(socket));
		}

		public String inputString(Socket socket) throws IOException {
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

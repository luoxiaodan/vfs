package func;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import vfs.struct.ChunkInfo;

public class Slave {

	public List<ChunkInfo> chunkInfoList = new ArrayList<ChunkInfo>();
	public List<Integer> chunkRent = new ArrayList<Integer>();
	public static final String CHUNK_LOG = "chunklog.txt";
	public static final String CHUNK_RENT = "chunkRentlog.txt";
	public static final int CHUNK_SIZE = 64 * 1024; // *1024;
	public static final int UPLOAD_BUFFER_SIZE = 8 * 1024;
	public static final int DOWNLOAD_BUFFER_SIZE = 8 * 1024;
	public static final int SLAVE_PORT = 8890;
	public String Slave_ip = "";

	public Slave() {
	};

	public String getSlaveIp() {
		return Slave_ip;
	}

	public void IniSalve() throws IOException {

		Slave_ip = InetAddress.getLocalHost().getHostAddress();
		;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;

		File fileName = new File(CHUNK_LOG);
		if (fileName.exists()) {
			try {
				fileReader = new FileReader(fileName);
				bufferedReader = new BufferedReader(fileReader);
				try {
					String read = "";
					while ((read = bufferedReader.readLine()) != null) {
						// System.out.println("line��"+"\r\n"+read);
						String[] Info = read.split(" ");
						ChunkInfo chunkInfo = new ChunkInfo(Integer.valueOf(Info[0]), Info[1], Integer.valueOf(Info[2]),
								Integer.valueOf(Info[3]), Integer.valueOf(Info[4]));
						chunkInfoList.add(chunkInfo);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (bufferedReader != null) {
					bufferedReader.close();
				}
				if (fileReader != null) {
					fileReader.close();
				}
			}
		}
		fileName = new File(CHUNK_RENT);
		if (fileName.exists()) {
			String read = "";
			while ((read = bufferedReader.readLine()) != null) {
				// System.out.println("line��"+"\r\n"+read);
				chunkRent.add(Integer.valueOf(read));
			}

		}

	}

	public boolean CreateChunk(JSONObject chunk) throws Exception {

		int chunkid = chunk.getInt("chunk_id");
		boolean isRent = chunk.getBoolean("is_rent");
		if (isRent) {
			chunkRent.add(chunkid);
			writeChunkRent(chunkid);
		}
		ChunkInfo chunkInfo = new ChunkInfo(chunkid, Slave_ip, SLAVE_PORT, 0, CHUNK_SIZE);
		chunkInfoList.add(chunkInfo);
		JSONArray copyid = chunk.getJSONArray("ids_of_copies");
		for (int i = 0; i < copyid.length(); i++) {
			chunkInfo = new ChunkInfo((int) copyid.get(i), Slave_ip, SLAVE_PORT, 0, CHUNK_SIZE);
			chunkInfoList.add(chunkInfo);
		}

		this.writeChunkLog(chunkInfo);
		return true;
	}

	public JSONArray getChunkList() {
		JSONArray chunkArray = new JSONArray();
		for (int i = 0; i < chunkInfoList.size(); i++) {

			ChunkInfo chunkInfo = chunkInfoList.get(i);
			JSONObject obj = new JSONObject();
			obj.put("chunk_id", chunkInfo.chunkId);
			obj.put("slave_ip", chunkInfo.slaveIP);
			obj.put("port", chunkInfo.port);
			obj.put("file_index", chunkInfo.fileIndex);
			obj.put("chunk_left", chunkInfo.chunkLeft);
			chunkArray.put(obj);

		}
		return chunkArray;
	}

	public boolean deleteChunk(int chunkid) throws Exception {
		boolean flagchunk = false;
		boolean flagchunkid = false;
		for (int i = 0; i < chunkInfoList.size(); i++) {
			ChunkInfo chunkInfo = chunkInfoList.get(i);
			if (chunkInfo.chunkId == chunkid) {
				chunkInfoList.remove(i);
				if (chunkRent.contains(chunkid)) {
					for (int j = 0; j < chunkRent.size(); j++) {
						if (chunkRent.get(j) == chunkid) {
							chunkRent.remove(j);
							flagchunkid = true;
							break;
						}
					}
				}
				flagchunk = true;
				break;
			}
		}
		if (flagchunk) {
			for (int i = 0; i < chunkInfoList.size(); i++) {
				ChunkInfo chunkInfo = chunkInfoList.get(i);
				File fileName = new File(CHUNK_LOG);
				if (fileName.exists()) {
					fileName.delete();
				}
				this.writeChunkLog(chunkInfo);
			}
		}
		if (flagchunkid) {
			for (int i = 0; i < chunkRent.size(); i++) {
				File fileName = new File(CHUNK_RENT);
				if (fileName.exists()) {
					fileName.delete();
				}
				writeChunkRent(chunkRent.get(i));
			}
		}
		return flagchunk;

	}

	public byte[] readChunk(int chunkid, int offset, int readLen) throws IOException {
		byte[] buffer = new byte[readLen];
		for (int i = 0; i < chunkInfoList.size(); i++) {
			ChunkInfo chunkInfo = chunkInfoList.get(i);
			if (chunkInfo.chunkId == chunkid) {

				String contentPath = "E:\\content" + Integer.toString(chunkid);
				File fileName = new File(contentPath);
				if (fileName.exists()) {
					FileInputStream in = new FileInputStream(contentPath);

					in.read(buffer, offset, readLen);

					in.close();

				}
				break;
			}
		}
		return buffer;
	}

	public boolean writeChunk(int chunkid, int offset, int writeLen, String content) {
		boolean flag = false;
		String contentPath = "E:\\content" + Integer.toString(chunkid);
		try {
			File fileName = new File(contentPath);
			if (!fileName.exists()) {
				fileName.createNewFile();
				System.out.println("CreateFile" + contentPath);
			}
			FileOutputStream out = new FileOutputStream(contentPath);

			out.write(content.getBytes(), offset, writeLen);
			out.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < chunkInfoList.size(); i++) {
			ChunkInfo chunkInfo = chunkInfoList.get(i);
			if (chunkInfo.chunkId == chunkid) {

				chunkInfo.chunkLeft -= writeLen;

				flag = true;
				break;
			}

		}
		return flag;
	}

	public void writeChunkLog(ChunkInfo chunkInfo) throws Exception {

		try {
			File fileName = new File(CHUNK_LOG);
			if (!fileName.exists()) {
				fileName.createNewFile();
				System.out.println("CreateFile" + CHUNK_LOG);
			}

			BufferedWriter output = new BufferedWriter(new FileWriter(fileName, true));

			String Info = chunkInfo.chunkId + " " + chunkInfo.slaveIP + " " + chunkInfo.port + " " + chunkInfo.fileIndex
					+ " " + chunkInfo.chunkLeft + "\n";

			output.write(Info);
			output.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void writeChunkRent(int chunkid) throws Exception {

		try {
			File fileName = new File(CHUNK_RENT);
			if (!fileName.exists()) {
				fileName.createNewFile();
				System.out.println("CreateFile" + CHUNK_RENT);
			}

			BufferedWriter output = new BufferedWriter(new FileWriter(fileName, true));

			String Info = chunkid + "\n";
			output.write(Info);
			output.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

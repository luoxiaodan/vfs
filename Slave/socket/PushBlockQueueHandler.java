package socket;

import java.io.IOException;

/**
 * 队列消息处理实现
 * @author hp
 *
 */
public class PushBlockQueueHandler implements Runnable {

    private Object obj;
    public PushBlockQueueHandler(Object obj){
        this.obj = obj;
    }
    
    @Override
    public void run() {
        try {
			doBusiness();
		} catch (InterruptedException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * 业务处理时限
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void doBusiness() throws InterruptedException, IOException{
        System.out.println(" work out "+obj );
        SlaveServer.signObj=(String)obj;
        SlaveServer.signWork="begin";
        while(!SlaveServer.signWork.equals("end")){
        	System.out.println("waiting");
        	
        }
    }

}
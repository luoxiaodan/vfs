package socket;
/**
 * ������Ϣ����ʵ��
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
        doBusiness();
    }
    
    /**
     * ҵ����ʱ��
     */
    public void doBusiness(){
        System.out.println(" work out "+obj );
        SlaveServer.signObj=(String)obj;
    }

}
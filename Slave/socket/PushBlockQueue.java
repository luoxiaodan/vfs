package socket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * ��Ϣ���л��嶨��
 * @author hp
 *
 */
public class PushBlockQueue extends LinkedBlockingQueue<Object>{
    
    private static final long serialVersionUID = -8224792866430647454L;
    private static ExecutorService es = Executors.newFixedThreadPool(10);//�̳߳�
    private static PushBlockQueue pbq = new PushBlockQueue();//����
    private boolean flag = false;
    
    private PushBlockQueue(){}
    
    public static PushBlockQueue getInstance(){
        return pbq;
    }
    
    /**
     * ���м�������
     */
    public void start(){
        if(!this.flag){
            this.flag = true;
        }else{
            throw new IllegalArgumentException("this queue is staing,don't start again.");
        }
        new Thread(new Runnable(){
            @Override
            public void run() {
                while(flag){
                    try {
                        Object obj = take();//ʹ������ģʽ��ȡ������Ϣ
                        //����ȡ��Ϣ�����̳߳ش���
                        es.execute(new PushBlockQueueHandler(obj));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        
    }
    
    /**
     * ֹͣ���м���
     */
    public void stop(){
        this.flag = false;
    }
}
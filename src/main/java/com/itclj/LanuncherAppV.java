package com.itclj;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Create by lujun.chen on 2018/09/10
 */
public class LanuncherAppV {

  public static void main(String[] args) throws IOException, InterruptedException {
    HashMap env = new HashMap();
    //这两个属性必须设置
    env.put("HADOOP_CONF_DIR", "/home/hadoop/app/hadoop-2.7.5/etc/hadoop");
    env.put("JAVA_HOME", "/usr/java/jdk1.8.0_161");
    //可以不设置
    //env.put("YARN_CONF_DIR","");
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在
    SparkAppHandle handle = new SparkLauncher(env)
        .setSparkHome("/home/hadoop/app/spark-2.2.1-bin-hadoop2.7")
        .setAppResource("/home/hadoop/SparkHelloWorld-1.1.jar")
        .setMainClass("com.itclj.JavaSparkPi")
        .setMaster("yarn")
        .setDeployMode("cluster")
        .setConf("spark.app.id", "itclj_20180910_01")
        .setConf("spark.driver.memory", "1g")//触发器内存大小
        .setConf("spark.akka.frameSize", "200")
        .setConf("spark.executor.memory", "1g")//执行器内存大小
        .setConf("spark.executor.instances", "2")//执行实例数
        .setConf("spark.executor.cores", "1")//CPU核数
        .setConf("spark.default.parallelism", "2")//并行数
        .setConf("spark.driver.allowMultipleContexts", "true")
        .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
          //这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
          @Override
          public void stateChanged(SparkAppHandle sparkAppHandle) {
            if (sparkAppHandle.getState().isFinal()) {
              countDownLatch.countDown();
            }
            System.out.println("state:" + sparkAppHandle.getState().toString());
          }


          @Override
          public void infoChanged(SparkAppHandle sparkAppHandle) {
            System.out.println("Info:" + sparkAppHandle.getState().toString());
          }
        });
    System.out.println("The task is executing, please wait ....");
    //线程等待任务结束
    countDownLatch.await();

    while(!"FINISHED".equalsIgnoreCase(handle.getState().toString()) && !"FAILED".equalsIgnoreCase(handle.getState().toString())){
      System.out.println("id    "+handle.getAppId());
      System.out.println("state "+handle.getState());

      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("The task is finished!");

  }
}

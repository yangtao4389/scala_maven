package spark1.project.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/*封装一个单例模式 具体用例用main来实现*/
public class HbaseUtils {
    private static HbaseUtils instance = null;
    private Connection connection = null;
    private Configuration configuration = null;
    private HbaseUtils(){
        try{
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","199.199.199.199:2181");
            configuration.set("hbase.rootdir", "hdfs://199.199.199.199:8020/hbase");
            connection = ConnectionFactory.createConnection(configuration);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /*
    * 获取HBase实例
    * */
    public static synchronized  HbaseUtils getInstance(){
        if(null == instance) {
            instance = new HbaseUtils();
        }
        return instance;
    }

    /*
    * 根据表名获取Htable实例
    * */
    public HTable getTable(String tableName){
        HTable hTable = null;
        try{
//            hTable = new HTable(connection,tableName);
            hTable = (HTable)connection.getTable(TableName.valueOf(tableName));
        }catch (Exception e){
            e.printStackTrace();
        }
        return hTable;
    }



    public void put(String tableName, String rowkey, String cf, String column, String value){
        HTable hTable = getTable(tableName);
        Put p = new Put(Bytes.toBytes(rowkey));
        p.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try{
            hTable.put(p);
            System.out.println("put down");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        String tableName = "ns1:courses_search_clickcount";
        HTable Student = HbaseUtils.getInstance().getTable(tableName);
//        Put p = new Put(Bytes.toBytes("row2"));
//        p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("some Value"));
//        Student.put(p);
//        System.out.println("down");
//
//        String rowkey = "rowkey1";
//        String cf = "info" ;
//        String column = "age";
//        String value = "2";

//        HbaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
    }



}

package spark1.project.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseExample {
    public static void main(String[] args) throws Exception{
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "199.199.199.199");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.rootdir", "hdfs://199.199.199.199:8020/hbase");
        Connection connection = ConnectionFactory.createConnection(config);

        try{
            Table table = connection.getTable(TableName.valueOf("member"));
            try{
                Put p = new Put(Bytes.toBytes("myLittelFamily"));
//                p.add(Bytes.toBytes("myLittleFamily"),Bytes.toBytes("someQualifier"),Bytes.toBytes("some Value"));
                p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("some Value"));
                table.put(p);

                Get g = new Get(Bytes.toBytes("myLittelFamily"));
                Result r = table.get(g);
                byte[] value = r.getValue(Bytes.toBytes("info"),Bytes.toBytes("age"));

                String valusStr = Bytes.toString(value);
                System.out.println("GET: "+valusStr);

                Scan s = new Scan();
                s.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"));
                ResultScanner scanner = table.getScanner(s);

                try{
                    for (Result rr = scanner.next(); rr != null; rr=scanner.next()){
                        System.out.println("Found row: "+ rr);
                    }
                }finally {
                    scanner.close();
                }

            }finally {
                if(table != null) table.close();
            }
        }finally {
            connection.close();
        }
    }



}

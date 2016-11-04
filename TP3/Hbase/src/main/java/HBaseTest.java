/**
 * Created by thomas on 11/2/16.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {
    private static Configuration conf = null;
    /**
     * Initialization
     */

    static {
        conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Delete a column
     */
    public static void delSpecificRecord(String tableName, String rowKey, String friend)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        Delete del = new Delete(rowKey.getBytes());
        del.deleteColumn(Bytes.toBytes("Friends"), Bytes.toBytes(friend));
        table.delete(del);
        System.out.println("del recored ok.");
    }

    /**
     * Get a row
     */
    public static int getOneRecord (String tableName, String rowKey, boolean print) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        int exist = 0;
        if (rs.size() == 0){
            System.out.println("Error, " + rowKey + " doesn't exist");
            exist = 0;
        } else {
            if (print){
                for(KeyValue kv : rs.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }

            }
            exist = 1;

        }
        return exist;
    }

    /**
     * Get a specific row
     */
    public static int getSpecificRecord (String tableName, String rowKey, String field) throws IOException{
        HTable table = new HTable(conf, tableName);
        int exist = 0;
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            String qualifier = new String(kv.getQualifier());
            if(qualifier.equals(field)){
                exist = 1;
            }
        }
        return exist;
    }
    
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
                System.out.println("");
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static int getBFF(String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        int bff = 0;
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            String value = new String(kv.getValue());
            if(value.equals("BFF")){
                bff = 1;
            }
        }
        return bff;

    }

    public static void main(String[] agrs) {
        try {

            String tableName = "tlorillon";
            String[] familys = { "Info", "Friends" };
            HBaseTest.creatTable(tableName, familys);

            HBaseTest.addRecord(tableName, "Pierre", "Info", "age", "25");
            HBaseTest.addRecord(tableName, "Pierre", "Info", "email", "pierre@hadoop.fr");
            HBaseTest.addRecord(tableName, "Pierre", "Friends", "Zeitoun", "BFF");
            HBaseTest.addRecord(tableName, "Pierre", "Friends", "Toto", "Others");
            HBaseTest.addRecord(tableName, "Pierre", "Friends", "Fred", "Others");
            HBaseTest.addRecord(tableName, "Pierre", "Friends", "Lisa", "Others");

            HBaseTest.addRecord(tableName, "Thomas", "Info", "age", "23");
            HBaseTest.addRecord(tableName, "Thomas", "Info", "email", "thomas@hadoop.fr");
            HBaseTest.addRecord(tableName, "Thomas", "Friends", "Antoine", "BFF");
            HBaseTest.addRecord(tableName, "Thomas", "Friends", "Tristan", "Others");
            HBaseTest.addRecord(tableName, "Thomas", "Friends", "Coco", "Others");
            HBaseTest.addRecord(tableName, "Thomas", "Friends", "Hugo", "Others");


            HBaseTest.addRecord(tableName, "Antoine", "Info", "age", "22");
            HBaseTest.addRecord(tableName, "Antoine", "Info", "email", "antoine@hadoop.fr");
            HBaseTest.addRecord(tableName, "Antoine", "Friends", "Tristan", "Others");
            HBaseTest.addRecord(tableName, "Antoine", "Friends", "Alice", "Others");
            HBaseTest.addRecord(tableName, "Antoine", "Friends", "Toto", "Others");

            Scanner sc = new Scanner(System.in);
            int choice = -1;
            do {
                int exist, bff;
                String user, friend, info;
                System.out.print("\n\t\t\tWelcome to your Social Network Account\n\n\n");
                System.out.print("\t1.Show All the Users");
                System.out.print("\t\t\t2.Show one user\n\n");
                System.out.print("\t3.Add a new Friend to a user");
                System.out.print("\t\t4.Remove a friend of a user\n\n");
                System.out.print("\t5.Modify Info of a user");
                System.out.print("\t\t\t6.Add a new user\n\n");
                System.out.print("\t7.Delete a user");
                System.out.print("\t\t\t\t8.Reinitialization of the database\n\n");
                System.out.println("\t\t\t\t0.Exit!");
                System.out.print("Choice (0-8) : ");
                try {
                    choice = sc.nextInt();
                    if ((choice >= 0) && (choice <= 8)) {
                        switch (choice) {
                            case 1:
                                System.out.println("===========Show All Records========");
                                HBaseTest.getAllRecord(tableName);
                                break;
                            case 2:
                                System.out.println("Enter user name to retrieve!");
                                user = sc.next();
                                System.out.println("===========Get One Record========");
                                HBaseTest.getOneRecord(tableName, user, true);
                                break;
                            case 3:
                                System.out.println("Enter the user you want to add a friend:");
                                user = sc.next();
                                exist = HBaseTest.getOneRecord(tableName, user, false);
                                if (exist == 1) {
                                    System.out.println("Enter the name of the friend:");
                                    friend = sc.next();
                                    System.out.println("Is he/she the BFF ?\n1.Yes\t2.No");
                                    choice = sc.nextInt();
                                    if (choice == 1) {
                                        bff = HBaseTest.getBFF(tableName, user);
                                        if (bff == 1) {
                                            System.out.println("Error, BFF for that user already exists...");
                                            break;
                                        } else {
                                            HBaseTest.addRecord(tableName, user, "Friends", friend, "BFF");
                                        }
                                    } else {
                                        HBaseTest.addRecord(tableName, user, "Friends", friend, "Others");
                                    }
                                }
                                break;
                            case 4:
                                System.out.println("Enter the user you want to delete a friend:");
                                user = sc.next();
                                exist = HBaseTest.getOneRecord(tableName, user, false);
                                if (exist == 1) {
                                    System.out.println("Enter the name of the friend to delete:");
                                    friend = sc.next();
                                    exist = HBaseTest.getSpecificRecord(tableName, user, friend);
                                    if (exist == 1) {
                                        delSpecificRecord(tableName, user, friend);
                                    } else {
                                        System.out.println("Error, " + user + " doesn't have a friend named " + friend);
                                    }
                                }
                                break;
                            case 5:
                                System.out.println("Which user do you want to modify ?");
                                user = sc.next();
                                exist = HBaseTest.getOneRecord(tableName, user, false);
                                if (exist == 1) {
                                    System.out.println("Which Info do you wish to modify ?");
                                    System.out.println("1. Age\t\t2.Email");
                                    choice = sc.nextInt();
                                    switch (choice) {
                                        case 1:
                                            System.out.print("What is the age of " + user + " ? ");
                                            String age = sc.next();
                                            HBaseTest.addRecord(tableName, user, "Info", "age", age);
                                            break;
                                        case 2:
                                            System.out.print("What is the email of " + user + " ? ");
                                            String email = sc.next();
                                            HBaseTest.addRecord(tableName, user, "Info", "email", email);
                                            break;
                                        default:
                                            System.out.println("Error, please enter correct value : 1 - 2");
                                            break;
                                    }
                                }
                                break;
                            case 6:
                                System.out.print("Enter username : ");
                                user = sc.next();
                                bff = 0;
                                exist = HBaseTest.getOneRecord(tableName, user, false);
                                if (exist == 0) {
                                    System.out.print("Enter user email : ");
                                    info = sc.next();
                                    HBaseTest.addRecord(tableName, user, "Info", "email", info);
                                    System.out.print("Enter user age : ");
                                    info = sc.next();
                                    HBaseTest.addRecord(tableName, user, "Info", "age", info);
                                    do {
                                        System.out.print("Enter the friend name : ");
                                        friend = sc.next();
                                        if (bff != 1) {
                                            System.out.println("Is he the BFF ?");
                                            System.out.println("1.Yes\t2.No");
                                            choice = sc.nextInt();
                                            switch (choice) {
                                                case 1:
                                                    HBaseTest.addRecord(tableName, user, "Friends", "BFF", friend);
                                                    System.out.println("BFF Added  :)!!");
                                                    bff = 1;
                                                    break;
                                                case 2:
                                                    HBaseTest.addRecord(tableName, user, "Friends", "Others", friend);
                                                    System.out.println("Friend Added :) !");
                                                    break;
                                                default:
                                                    System.out.println("Invalid Choice, try again");
                                                    break;
                                            }

                                        } else {
                                            HBaseTest.addRecord(tableName, user, "Friends", "Others", friend);
                                        }
                                        System.out.println("Do you wish to continue ?");
                                        System.out.println("1.Yes\t2.No");
                                        choice = sc.nextInt();
                                    } while (choice != 2);
                                } else {
                                    System.out.println("Error, " + user + " already exist");
                                }
                                break;
                            case 7:
                                System.out.print("Enter user to Delete : ");
                                user = sc.next();
                                exist = HBaseTest.getOneRecord(tableName, user, true);
                                if (exist == 1) {
                                    System.out.println("Are you sure you want to suppress that user ?");
                                    System.out.println("1.Yes\t2.No");
                                    choice = sc.nextInt();
                                    switch (choice) {
                                        case 1:
                                            HBaseTest.delRecord(tableName, user);
                                            break;
                                        case 2:
                                            break;
                                        default:
                                            System.out.println("Invalid Choice, try again");
                                            break;
                                    }
                                }
                                break;
                            case 8:
                                System.out.println("Are you sure you want to reinitialize the database (it will exit the program) ? ");
                                System.out.print("1.Yes\t2.No\n");
                                System.out.print("Choice : ");
                                choice = sc.nextInt();
                                if (choice == 1){
                                    deleteTable(tableName);
                                    System.out.println("Goodbye, See you soon !");
                                    choice = 0;
                                }
                                break;
                            case 0:
                                System.out.println("Bye, See you soon :)");
                                break;
                            default:
                                System.out.println("Invalid Choice, try again");
                                break;

                        }
                    } else {
                        System.out.println("Please enter a valid choice! (Value between 0 and 8)");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    sc.next();

                }
            }while(choice !=0);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

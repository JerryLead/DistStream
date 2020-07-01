import java.io.*;

/**
 * Created by Chongrui on 2018/6/6 0006.
 */
public class DivideFlie {
    public static void main(String[] args) throws IOException {
//        String dataPath = "e:\\data\\kddcup.data_10_percent_corrected_train_normalized.csv";
//        String output = "e:\\data\\kddccup.data_10_percent_train_normalized.csv";
        String dataPath = "/Users/kk/Downloads/lcr/data/data123/kddcup.data_10_percent_corrected_train_normalized.csv";
        String output = "/Users/kk/Downloads/lcr/data/data123/kddcup.data_10_percent_train_normalized.csv";
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(dataPath)));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));
        String str = "";
        int count = 0;
        while ((str = in.readLine()) != null){
            count++;
            if (count <= 434021) out.write(str+"\n");
        }
        System.out.println(count);
        out.flush();
        out.close();
    }
}

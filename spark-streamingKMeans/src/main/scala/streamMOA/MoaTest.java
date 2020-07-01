package streamMOA;

import com.yahoo.labs.samoa.instances.Attribute;

import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.cluster.SphereCluster;
import moa.clusterers.denstream.WithDBSCAN;
import moa.core.AutoExpandVector;
import moa.core.InstanceExample;
import moa.core.TimingUtils;
import moa.evaluation.CMM;
import moa.gui.visualization.DataPoint;
import moa.streams.clustering.SimpleCSVStream;
import com.yahoo.labs.samoa.instances.Instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static moa.cluster.Clustering.classValues;

public class MoaTest {

    private AutoExpandVector<Cluster> clusters;

    public AutoExpandVector<Cluster> getClusters() {
        return clusters;
    }

    public void Clustering(List<Instance> points){
        HashMap<Integer, Integer> labelMap = classValues(points);
        int dim = points.get(0).dataset().numAttributes()-1;

        int numClasses = labelMap.size();
        int noiseLabel;

        Attribute classLabel = points.get(0).dataset().classAttribute();
        int lastLabelIndex = classLabel.numValues() - 1;
        //if (classLabel.value(lastLabelIndex) == "noise") {
          //  noiseLabel = lastLabelIndex;
        //} else {
            noiseLabel = -1;
        //}

        ArrayList<com.yahoo.labs.samoa.instances.Instance>[] sorted_points = (ArrayList<com.yahoo.labs.samoa.instances.Instance>[]) new ArrayList[numClasses];
        for (int i = 0; i < numClasses; i++) {
            sorted_points[i] = new ArrayList<com.yahoo.labs.samoa.instances.Instance>();
        }
        for (com.yahoo.labs.samoa.instances.Instance point : points) {
            int clusterid = (int)point.classValue();
            if(clusterid == noiseLabel) continue;
            sorted_points[labelMap.get(clusterid)].add((com.yahoo.labs.samoa.instances.Instance)point);
        }
        this.clusters = new AutoExpandVector<Cluster>();
        for (int i = 0; i < numClasses; i++) {
            if(sorted_points[i].size()>0){
                SphereCluster s = new SphereCluster(sorted_points[i],dim);
                s.setId(sorted_points[i].get(0).classValue());
                s.setGroundTruth(sorted_points[i].get(0).classValue());
                clusters.add(s);
            }
        }
    }

    public  static  void main(String[] args) throws Exception {



        String inputPath = "/Users/yxtwkk/Documents/ISCAS/smallDataSet.csv";
        //String hdfsPath  = "/Users/yxtwkk/Documents/ISCAS/G/data.csv";

        float offline = 2.0f;
        float mu = 1.0f;
        float lambda = 0.25f;
        float epsilon = 0.02f;
        int speedRate = 1000;
        int initialPoints = 10;
        float beta = 0.2f;
        boolean tag = true;



        if (args.length > 0) inputPath = args[0];
        if (args.length > 1) tag = Boolean.parseBoolean(args[1]);
        if (args.length > 2) offline = Float.parseFloat(args[2]);
        if (args.length > 3) mu = Float.parseFloat(args[3]);
        if (args.length > 4) lambda = Float.parseFloat(args[4]);
        if (args.length > 5) epsilon = Float.parseFloat(args[5]);
        if (args.length > 6) speedRate = Integer.parseInt(args[6]);
        if (args.length > 7) initialPoints = Integer.parseInt(args[7]);
        if (args.length > 8) beta = Float.parseFloat(args[8]);

        WithDBSCAN denstream = new WithDBSCAN();
        SimpleCSVStream stream = new SimpleCSVStream();

        stream.csvFileOption.setValue(inputPath);
        stream.classIndexOption.setValue(tag);

        denstream.offlineOption.setValue(offline);
        denstream.muOption.setValue(mu);
        denstream.betaOption.setValue(beta);
        denstream.epsilonOption.setValue(epsilon);
        denstream.speedOption.setValue(speedRate);
        denstream.initPointsOption.setValue(initialPoints);


        denstream.prepareForUse();
        stream.prepareForUse();


        TimingUtils preciseCPUTiming = new TimingUtils();
        preciseCPUTiming.enablePreciseTiming();
        TimingUtils evaluateStartTime = new  TimingUtils();
        Long time = evaluateStartTime.getNanoCPUTimeOfCurrentThread();

        int numPoints = 0;

        List dataInstance = new ArrayList();
        int timestamp = 0;
        ArrayList<DataPoint> points = new ArrayList<DataPoint>();

        while(stream.hasMoreInstances()){

            InstanceExample trainInst = stream.nextInstance();
            System.out.println(trainInst.toString());
            denstream.trainOnInstance(trainInst.getData());
            //dataInstance.add(trainInst.getData());
            //numPoints ++;
            //DataPoint d = new DataPoint(trainInst.getData(),timestamp);
            //System.out.println("数据时间戳是"+d.getTimestamp());
            //System.out.println("数据是否是噪声"+d.isNoise());
            /*if(numPoints % speedRate == 0)
            {
                timestamp++;
                //System.out.println(timestamp);
            }*/
            //timestamp++;
     //       points.add(d);

        }

        Clustering middleResult = denstream.getMicroClusteringResult();
        for(int i = 0;i<middleResult.size();i++){

            System.out.println(middleResult.get(i).getWeight());
        }

        Clustering  finalResult = denstream.getClusteringResult();

        for(int i = 0;i<finalResult.size();i++){

            System.out.println(finalResult.get(i).getWeight());
        }



/*        Clustering trueCluters = new Clustering(dataInstance);

         System.out.println("聚类个数：");
        System.out.println(trueCluters.getClustering().size());
        for(int j =0;j<trueCluters.getClustering().size();j++){

            System.out.println(trueCluters.getClustering().get(j).getGroundTruth());

            System.out.println(trueCluters.getClustering().get(j).getWeight());
        }*/



        /*MoaTest moaTest =new MoaTest();
        moaTest.Clustering(dataInstance);
        System.out.println("聚类个数：");
        System.out.println(moaTest.getClusters().size());
        for(int j =0;j<moaTest.getClusters().size();j++){

            System.out.println(moaTest.getClusters().get(j).getGroundTruth());

            System.out.println(moaTest.getClusters().get(j).getWeight());
        }
        //CMM cmm = new CMM();
        //cmm.evaluateClustering(finalResult,trueCluters,points);*/



    }
}

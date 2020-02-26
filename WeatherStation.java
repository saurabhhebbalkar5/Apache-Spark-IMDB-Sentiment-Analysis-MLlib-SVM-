package saurabh_assignment3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Saurabh
 */
public class WeatherStation implements Serializable {

    //Creating custom list of Measurement class
    List<Measurement> measurements = new ArrayList<Measurement>();

    //Creating custom static WeatherStation list
    static List<WeatherStation> stations = new ArrayList<WeatherStation>();

    String cityname;

    //Weather Station Constructor with city name and measurement list
    WeatherStation(String cityname, List<Measurement> measurements) {

        this.cityname = cityname;
        this.measurements = measurements;
    }

    //countTemperature function
    public static long countTemperature(double t1) {

        //Spark Configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("WeatherStation")
                .setMaster("local[4]").set("spark.executor.memory", "1g");

        //Intializing spark context
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        //Creating distributed dataset with partitions using parallelize
        JavaRDD<WeatherStation> st = ctx.parallelize(stations);

        //Filtering the temperature which are +1/-1 from the temperature passed in the countTemperature
        JavaRDD<Measurement> temp = st.flatMap(f -> f.measurements.iterator())
                .filter(t -> ((t1 + 1 >= t.temperature) && (t1 - 1 <= t.temperature)));

        //Getting the count and returning the value
        long result = temp.count();
        return result;
    }

    //Main method
    public static void main(String args[]) {

        //Pune City Measurement List
        List<Measurement> pune = new ArrayList<Measurement>();
        pune.add(new Measurement(10, 20.0));
        pune.add(new Measurement(15, 5.0));
        pune.add(new Measurement(20, 30.0));
        pune.add(new Measurement(25, 21.0));
        pune.add(new Measurement(30, 17.0));

        //Mumbai City Measurement List
        List<Measurement> mumbai = new ArrayList<Measurement>();
        mumbai.add(new Measurement(12, 1.0));
        mumbai.add(new Measurement(17, 4.0));
        mumbai.add(new Measurement(22, 9.0));
        mumbai.add(new Measurement(26, 12.0));
        mumbai.add(new Measurement(29, 20.0));

        //Creating WeatherStation object and passing the city data
        WeatherStation ws1 = new WeatherStation("Pune", pune);
        stations.add(ws1);

        WeatherStation ws2 = new WeatherStation("Mumbai", mumbai);
        stations.add(ws2);

        //Printing the results by calling the countTemperature function.
        System.out.println("\n\n" + "The temperature has occured:\n\n" + countTemperature(5.0) + " times");
        System.out.println("\n\n");

    }
}

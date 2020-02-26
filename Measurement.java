package saurabh_assignment3;

import java.io.Serializable;

/**
 *
 * @author Saurabh
 */
public class Measurement implements Serializable {

    int time;
    double temperature;

    //Constructor initiating objects
    Measurement(int time, double temperature) {

        this.time = time;
        this.temperature = temperature;
    }

}

package sparkJoinData;

import java.io.Serializable;

public class AirportStatistic implements Serializable {
    private double maxDelay;
    private int count;
    private int countDelay;
    private int countCanselled;

    public AirportStatistic(double delay, boolean isCanselled) {
        count = 1;
        maxDelay = delay;
        if (delay > 0.0) {
            countDelay = 1;
        }
        if (isCanselled) {
            countCanselled = 1;
        }
    }
    public AirportStatistic(AirportStatistic s1, AirportStatistic s2) {
        maxDelay = s1.getMaxDelay() > s2.getMaxDelay() ? s1.getMaxDelay() : s2.getMaxDelay();
        count = s1.getCount() + s2.getCount();
        countDelay = s1.getCountDelay() + s2.getCountDelay();
        countCanselled = s1.getCountCanselled() + s2.getCountCanselled();
    }

    public int getCountDelay() {
        return countDelay;
    }

    public int getCountCanselled() {
        return countCanselled;
    }

    public int getCount() {
        return count;
    }

    public double getMaxDelay() {
        return maxDelay;
    }
}

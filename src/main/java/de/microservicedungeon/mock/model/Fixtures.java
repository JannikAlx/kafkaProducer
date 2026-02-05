package de.microservicedungeon.mock.model;

import de.microservicedungeon.mock.model.map.Coordinate;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class Fixtures {
    public static int ROBOT_PRICE=100;

    private static final Random random = new Random();
    public static Coordinate randomPosition(){
        return new Coordinate(random.nextInt(40)+1, random.nextInt(40)+1);
    }
}

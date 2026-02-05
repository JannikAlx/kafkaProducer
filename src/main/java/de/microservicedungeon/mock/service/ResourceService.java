package de.microservicedungeon.mock.service;

import de.microservicedungeon.mock.model.Resource;
import de.microservicedungeon.mock.model.ResourceType;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ResourceService {

    private final Random random =new Random();

    private final ConcurrentHashMap<ResourceType, Integer> resourcePrices = new ConcurrentHashMap<>();

    public ResourceService(){
        resourcePrices.put(ResourceType.BIO_MATTER, 50);
        resourcePrices.put(ResourceType.CRYO_GAS, 100);
        resourcePrices.put(ResourceType.DARK_MATTER, 200);
        resourcePrices.put(ResourceType.ION_DUST, 400);
        resourcePrices.put(ResourceType.PLASMA_CORES, 800);
    }

    /**
     * Get a random resource type name.
     */
    public ResourceType getRandomResourceType() {
        ResourceType[] types = ResourceType.values();
        return types[random.nextInt(types.length)];
    }

    public int getPriceForResource(Resource resource){
        return resourcePrices.get(resource.type());
    }
}

package de.microservicedungeon.mock.model.map;

public record StarSystem(
        Coordinate coordinate,
        Integer gravity,
        SpaceStation spaceStation,
        Mine mine,
        Boolean blackHole,
        Boolean voidSystem
) {
    public String getType(){
        if (spaceStation != null){
            return "SPACE_STATION";
        }
        if (mine != null){
            return mine.type().name();
        }
        if (blackHole){
            return "BLACK_HOLE";
        }
        if (voidSystem){
            return "VOID";
        }
        else return "EMPTY";
    }
}

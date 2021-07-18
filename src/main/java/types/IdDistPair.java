package types;

public class IdDistPair implements Comparable<IdDistPair>{
    public int id;
    public float dist;

    public IdDistPair(int id, float dist)
    {
        this.id = id;
        this.dist = dist;
    }

    @Override
    public int compareTo(IdDistPair other) {
        if(dist != other.dist)
            return Float.compare(dist, other.dist);
        else if(id != other.id)
            return Integer.compare(id, other.id);
        else
            return -1;
    }
}

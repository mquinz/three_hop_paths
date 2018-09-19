package com.maxdemarzi;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Procedures {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    static final String WEIGHT = "weight";
    static final String TYPE = "type";
    static final Pair<List<Map<String, Object>>, Double> HEAVY =
            Pair.of(new ArrayList<Map<String, Object>>() {{
                add(null);
                add(null);
                add(null);
                add(null);
                add(null);
                add(null);
                add(null);
                add(null);
                add(null);
            }}, 9999999.9);

    // This field is static and gives us the ability to cache node and relationship properties
    static GraphDatabaseService graph;

    static final LoadingCache<Long, Map<String, Object>> nodeProperties = Caffeine.newBuilder()
            .maximumSize(1_000_000)
            .expireAfterWrite(1, TimeUnit.DAYS)
            .build(Procedures::getProperties);

    private static Map<String, Object> getProperties(Long key) {
        Node node = graph.getNodeById(key);
        return node.getAllProperties();
    }

    static final LoadingCache<Long, Map<String, Object>> relProperties = Caffeine.newBuilder()
            .maximumSize(10_000_000)
            .expireAfterWrite(1, TimeUnit.DAYS)
            .build(Procedures::getRelProperties);

    private static Map<String, Object> getRelProperties(Long key) {
        Relationship rel = graph.getRelationshipById(key);
        return rel.getAllProperties();
    }
    @Procedure(name = "com.maxdemarzi.three_hop_paths", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.three_hop_paths([from], [to], {weights}) - traverse paths")
    public Stream<WeightedListMapResult> threeHopPaths(@Name("from") List<String> from,
                                                       @Name("to") List<String> to,
                                                       @Name("weights") Map<String, Double> weights) {
        if (graph == null) {
            graph = db;
        }

        // Step 1: Find the from and to nodes
        List<Node> fromNodes = new ArrayList<>();
        List<Node> toNodes = new ArrayList<>();
        LongOpenHashSet toIds = new LongOpenHashSet();

        for (String id : from) {
            Node node = db.findNode(Labels.Node, "id", id);
            if (node != null) {
                fromNodes.add(node);
            }
        }
        for (String id : to) {
            Node node = db.findNode(Labels.Node, "id", id);
            if (node != null) {
                toNodes.add(node);
                toIds.add(node.getId());
            }
        }

        // Build a dictionary for the first and fourth nodes
        Long2IntOpenHashMap fromDictionary = new Long2IntOpenHashMap();
        Long2IntOpenHashMap toDictionary = new Long2IntOpenHashMap();
        Int2LongOpenHashMap reverseFromDictionary = new Int2LongOpenHashMap();
        Int2LongOpenHashMap reverseToDictionary = new Int2LongOpenHashMap();
        int fromCounter = 0;
        for (Node fromNode : fromNodes) {
            fromDictionary.put(fromNode.getId(), fromCounter);
            reverseFromDictionary.put(fromCounter, fromNode.getId());
            fromCounter++;
        }
        int toCounter = 0;
        for (Node toNode : toNodes) {
            toDictionary.put(toNode.getId(), toCounter);
            reverseToDictionary.put(toCounter, toNode.getId());
            toCounter++;
        }
        int forNodeMultiplier = toNodes.size();

        // Store all values in O(1) access arrays
        double[] values = new double[fromNodes.size() * toNodes.size()];
        int[] lengths = new int[fromNodes.size() * toNodes.size()];
        Object[] bestPaths = new Object[fromNodes.size() * toNodes.size()];
        RoaringBitmap pathsFound = new RoaringBitmap();

        for(int i = 0; i < values.length; i++) {
            values[i] = 9999999.0d;
            lengths[i] = 99;
        }

        // Keep track of Bi-Directional hops
        HashMap<Node, ArrayList<Triple<Integer, Double, String>>> firstHop = new HashMap<>();
        LongOpenHashSet secondNodeIds = new LongOpenHashSet();
        HashMap<Node, ArrayList<Triple<Integer, Double, String>>> thirdHop = new HashMap<>();

        // From nodes to first hop
        for (Node node : fromNodes) {
            long nodeId = node.getId();
            for (Map.Entry<String, Double> weight : weights.entrySet()) {
                double multiplier = weight.getValue();
                String relationshipType = weight.getKey();
                for (Relationship r : node.getRelationships(Direction.BOTH, RelationshipType.withName(relationshipType))) {
                    long rId = r.getId();
                    double distance = (double) relProperties.get(rId).get(WEIGHT) * multiplier;
                    long toNodeId = r.getOtherNodeId(nodeId);
                    if (toIds.contains(toNodeId)) {
                        int key = (forNodeMultiplier * fromDictionary.get(nodeId)) + toDictionary.get(toNodeId);
                        if (isBetter(values, lengths, pathsFound, key, 3, distance)) {
                            List<Map<String, Object>> path = new ArrayList<>();
                            path.add(nodeProperties.get(nodeId));
                            path.add(new HashMap<String, Object>() {{
                                put(WEIGHT, distance);
                                put(TYPE, relationshipType);
                            }});
                            path.add(nodeProperties.get(toNodeId));
                            bestPaths[key] = path;
                        }
                    }
                    if (firstHop.containsKey(r.getOtherNode(node))) {
                        firstHop.get(r.getOtherNode(node)).add(Triple.of(fromDictionary.get(nodeId), distance, relationshipType));
                    } else {
                        firstHop.put(r.getOtherNode(node), new ArrayList<Triple<Integer, Double, String>>() {{ add(Triple.of(fromDictionary.get(nodeId), distance, relationshipType));}});
                    }
                    secondNodeIds.add(toNodeId);
                }
            }
        }

        // Paths of length 2, starting from the TO nodes.
        for (Node node : toNodes) {
            long nodeId = node.getId();
            int nodeIdDictionary = toDictionary.get(nodeId);
            for (Map.Entry<String, Double> weight : weights.entrySet()) {
                double multiplier = weight.getValue();
                String relationshipType = weight.getKey();
                for (Relationship r : node.getRelationships(Direction.BOTH, RelationshipType.withName(relationshipType))) {
                    long thirdNodeId = r.getOtherNodeId(nodeId);
                    // No relationships to myself
                    if (thirdNodeId != nodeId) {
                        Node thirdNode = db.getNodeById(thirdNodeId);
                        double distance = (Double) relProperties.get(r.getId()).get(WEIGHT) * multiplier;

                        for (Triple<Integer, Double, String> firstTriple : firstHop.getOrDefault(thirdNode, new ArrayList<>())) {
                            long firstNodeId = reverseFromDictionary.get((int)firstTriple.getLeft());
                            // force check
                            if (firstNodeId != thirdNodeId && firstNodeId != nodeId) {
                                int key = (forNodeMultiplier * firstTriple.getLeft()) + nodeIdDictionary;
                                if (isBetter(values, lengths, pathsFound, key, 5, distance + firstTriple.getMiddle())) {
                                    List<Map<String, Object>> path = new ArrayList<>();

                                    // First Node
                                    path.add(nodeProperties.get(firstNodeId));
                                    path.add(new HashMap<String, Object>() {{
                                        put(WEIGHT, firstTriple.getMiddle());
                                        put(TYPE, firstTriple.getRight());
                                    }});

                                    // Middle Node
                                    path.add(nodeProperties.get(thirdNodeId));
                                    path.add(new HashMap<String, Object>() {{
                                        put(WEIGHT, distance);
                                        put(TYPE, relationshipType);
                                    }});
                                    // To Node
                                    path.add(nodeProperties.get(nodeId));
                                    bestPaths[key] = path;
                                }
                            }
                        }

                        if (thirdHop.containsKey(thirdNode)) {
                            thirdHop.get(thirdNode).add(Triple.of(toDictionary.get(nodeId), distance, relationshipType));
                        } else {
                            thirdHop.put(thirdNode, new ArrayList<Triple<Integer, Double, String>>() {{ add(Triple.of(toDictionary.get(nodeId), distance, relationshipType));}});
                        }
                    }
                }
            }
        }
        // Paths of length 3, meet in the middle from the third node
        for (Map.Entry<Node, ArrayList<Triple<Integer, Double, String>>> entry : thirdHop.entrySet()) {
            Node thirdNode = entry.getKey();
            long thirdNodeId = thirdNode.getId();
            // For all the different types
            for (Map.Entry<String, Double> weight : weights.entrySet()) {
                double multiplier = weight.getValue();
                String relationshipType = weight.getKey();
                // Traverse the relationships of the third node just once
                for (Relationship r : thirdNode.getRelationships(Direction.BOTH, RelationshipType.withName(relationshipType))) {
                    Node secondNode = r.getOtherNode(thirdNode);
                    long secondNodeId = secondNode.getId();
                    if (secondNodeIds.contains(secondNodeId)) {
                        // We have a connected path
                        long rId = r.getId();
                        double distance = (double) relProperties.get(rId).get(WEIGHT) * multiplier;
                        if (thirdNodeId != secondNodeId) {
                            // NOTES: get rid of iterators
                            // For all the to nodes at the end of the third hop
                            ArrayList<Triple<Integer, Double, String>> entries = entry.getValue();
                            for(int x = 0; x < entries.size(); x++) {
                                Triple<Integer, Double, String> toNodeTriple = entries.get(x);
                                long nodeId = reverseToDictionary.get((int)toNodeTriple.getLeft());
                                if (thirdNodeId != nodeId && secondNodeId != nodeId) {
                                    double thirdWeight = toNodeTriple.getMiddle();
                                    String thirdType = toNodeTriple.getRight();
                                    // For all the first nodes at the beginning of the first hop
                                    ArrayList<Triple<Integer, Double, String>> firstHops = firstHop.get(secondNode);
                                    for(int y = 0; y < firstHops.size(); y++) {
                                        Triple<Integer, Double, String> firstTriple = firstHops.get(y);
                                        int key = (forNodeMultiplier * firstTriple.getLeft()) + toNodeTriple.getLeft();
                                        //long key = (firstNodeId << 32) | nodeId;
                                        if (isBetter(values, lengths, pathsFound, key, 7, firstTriple.getMiddle() + thirdWeight + distance)) {
                                            List<Map<String, Object>> path = new ArrayList<>();

                                            // First Node
                                            path.add(nodeProperties.get(reverseFromDictionary.get((int)firstTriple.getLeft())));
                                            path.add(new HashMap<String, Object>() {{
                                                put(WEIGHT, firstTriple.getMiddle());
                                                put(TYPE, firstTriple.getRight());
                                            }});

                                            // Second Node
                                            path.add(nodeProperties.get(secondNodeId));
                                            path.add(new HashMap<String, Object>() {{
                                                put(WEIGHT, distance);
                                                put(TYPE, relationshipType);
                                            }});

                                            // Third Node
                                            path.add(nodeProperties.get(thirdNodeId));
                                            path.add(new HashMap<String, Object>() {{
                                                put(WEIGHT, thirdWeight);
                                                put(TYPE, thirdType);
                                            }});
                                            // Fourth Node
                                            path.add(nodeProperties.get(nodeId));
                                            bestPaths[key] = path;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        ArrayList<WeightedListMapResult> results = new ArrayList<>();
        IntIterator iterator = pathsFound.getIntIterator();
        while (iterator.hasNext()) {
            int location = iterator.next();
            results.add(new WeightedListMapResult((List)bestPaths[location], values[location]));
        }

        // Return only unique from-to paths, winner is lowest weight, if weigh is equal, then shortest path
        return results.stream();
    }

    private boolean isBetter(double[] values, int[] lengths, RoaringBitmap pathsFound, int location, int length, double weight) {
        if (values[location] > weight) {
            values[location] = weight;
            lengths[location] = length;
            pathsFound.add(location);
            return true;
        }
        if (values[location] == weight && (length < lengths[location])) {
            values[location] = weight;
            lengths[location] = length;
            pathsFound.add(location);
            return true;
        }

        return false;
    }
}

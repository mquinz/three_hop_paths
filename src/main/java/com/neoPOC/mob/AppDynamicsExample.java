package com.neoPOC.mob;

import com.google.gson.Gson;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


public class AppDynamicsExample {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    // Used to display the contents of structures like maps & arrays.
    // private static Gson gson = new Gson();

    public static String getNodeAsJson(Node node) {
        Iterable<org.neo4j.graphdb.Label> labels = node.getLabels();
        Map<String, Object> mProps = node.getAllProperties();
        mProps.put("nodeId", node.getId());
        //	return ( "nodeId " + node.getId()  + " " + labels);
        return new Gson().toJson(mProps);
    }

    @Description("com.neoPOC.getMobNames(node) | Return key value pairs of parents and (optional) children")
    @Procedure(name = "com.neoPOC.getMobNames", mode = Mode.READ)

    public Stream<KeyValueResult> getMobNames(@Name("startNode") Node startNode) {

//		System.out.println("begin getMobNames");
//		System.out.println(" startNodeId " + startNode.getId());
        HashMap<String, ArrayList<String>> hmNodes = new HashMap<String, ArrayList<String>>();

        long start = System.currentTimeMillis();

        try {
            for (Path position : db.traversalDescription()
                    .depthFirst()
                    .uniqueness(Uniqueness.NODE_PATH)
                    .relationships(Rels.ImpactEdge, Direction.OUTGOING)
                    .traverse(startNode)) {


                Relationship lastRel = position.lastRelationship();

                ArrayList<String> alChildren;
                if (position.length() > 0) {
                    Node toNode = lastRel.getEndNode();
                    Node fromNode = lastRel.getStartNode();
                    String fromName = (String) fromNode.getProperty("qName");
                    String toName = (String) toNode.getProperty("qName");

//				System.out.println(" fromName " +  fromName + " to " + toName);

                    if (hmNodes.containsKey(fromName)) {
                        alChildren = hmNodes.get(fromName);
                    } else {
                        alChildren = new ArrayList<String>();
                    }
                    // load an empty arraylist for the to node so it doesn't get left out
                    if (!hmNodes.containsKey(toName)) {
                        hmNodes.put(toName, new ArrayList<String>());
                    }
                    // to ensure that the children are unique
                    if (!alChildren.contains(toName)) {
                        alChildren.add(toName);
                        hmNodes.put(fromName, alChildren);
                    }
                } else {
                    //	System.out.println(" skipping  rel " + position.length());
                }

            }
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        //	System.out.println(" hmNodes " + new Gson().toJson(hmNodes));
        long end = System.currentTimeMillis();
        log.debug(" number of parents  =  " + hmNodes.size());
        log.debug(" traversal elapsed =  " + (end - start));

        return hmNodes.entrySet().stream().map(entry -> new KeyValueResult(entry.getKey(), entry.getValue()));
    }

    @Description("com.neoPOC.getMobNodes(node) | Return key value pairs of parents and (optional) children")
    @Procedure(name = "com.neoPOC.getMobNodes", mode = Mode.READ)

    public Stream<KeyValueResult> getMobNodes(@Name("startNode") Node startNode) {

//		System.out.println("begin getMobNodes");
//		System.out.println(" startNodeId " + startNode.getId());
        HashMap<Node, ArrayList<String>> hmNodes = new HashMap<Node, ArrayList<String>>();

        long start = System.currentTimeMillis();


        log.debug("this is a debug message");
        log.info("this is a info message");
        log.warn("this is a warn message");
        log.error("this is a error message");


        for (Path position : db.traversalDescription()
                .depthFirst()
                .uniqueness(Uniqueness.NODE_PATH)
                .relationships(Rels.ImpactEdge, Direction.OUTGOING)
                .traverse(startNode)) {

            Relationship lastRel = position.lastRelationship();

            ArrayList<String> alChildren;
            if (position.length() > 0) {
                Node toNode = lastRel.getEndNode();
                Node fromNode = lastRel.getStartNode();
                //	String fromName = (String) fromNode.getProperty("qName");
                String toName = (String) toNode.getProperty("qName");

                //			System.out.println(" fromName " +  fromName + " to " + toName);

                if (hmNodes.containsKey(fromNode)) {
                    alChildren = hmNodes.get(fromNode);
                } else {
                    alChildren = new ArrayList<String>();
                }
                // load an empty arraylist for the to node so it doesn't get left out
                if (!hmNodes.containsKey(toName)) {
                    hmNodes.put(toNode, new ArrayList<String>());
                }
                // to ensure that the children are unique
                if (!alChildren.contains(toName)) {
                    alChildren.add(toName);
                    hmNodes.put(fromNode, alChildren);
                }

            }

        }

        //		System.out.println(" hmNodes " + new Gson().toJson(hmNodes));
        long end = System.currentTimeMillis();
        log.debug(" number of parents  =  " + hmNodes.size());
        log.debug(" traversal elapsed =  " + (end - start));

        return hmNodes.entrySet().stream().map(entry -> new KeyValueResult(entry.getKey(), entry.getValue()));
    }

    @Description("com.neoPOC.getTimeMobName(node,time) | Return key value pairs of parents and (optional) children")
    @Procedure(name = "com.neoPOC.getTimeMobNames", mode = Mode.READ)

    public Stream<KeyValueResult> getTimeMobNames(@Name("startNode") Node startNode, @Name("time") Long timeValue, @Name("continue") Boolean continueOnExclude, @Name("uniqueness") String uniqueness) {

//		System.out.println("begin getMobNames");
//		System.out.println(" startNodeId " + startNode.getId());
        HashMap<String, ArrayList<String>> hmNodes = new HashMap<String, ArrayList<String>>();


        Uniqueness uniquenessType = Uniqueness.NODE_GLOBAL;

        switch (uniqueness) {
            case "NODE_LEVEL":
                uniquenessType = Uniqueness.NODE_LEVEL;
                break;
            case "NODE_PATH":
                uniquenessType = Uniqueness.NODE_PATH;
                break;
            case "NODE_RECENT":
                uniquenessType = Uniqueness.NODE_RECENT;
                break;
            case "NONE":
                uniquenessType = Uniqueness.NONE;
                break;
            case "RELATIONSHIP_GLOBAL":
                uniquenessType = Uniqueness.RELATIONSHIP_GLOBAL;
                break;
            case "RELATIONSHIP_LEVEL":
                uniquenessType = Uniqueness.RELATIONSHIP_LEVEL;
                break;
            case "RELATIONSHIP_PATH":
                uniquenessType = Uniqueness.RELATIONSHIP_PATH;
                break;
            case "RELATIONSHIP_RECENT":
                uniquenessType = Uniqueness.RELATIONSHIP_RECENT;
                break;
            default:
                System.out.println("no match");
                uniquenessType = Uniqueness.NODE_GLOBAL;
        }

        System.out.println("uniquenessType = " + uniquenessType);
        System.out.println("time = " + timeValue);
        System.out.println("startNode.id = " + startNode.getId());
        System.out.println("continueOnExclude = " + continueOnExclude);


        TimeValueEvaluator timeValueEvaluator = new TimeValueEvaluator(timeValue, continueOnExclude);


        long start = System.currentTimeMillis();

        for (Path position : db.traversalDescription()
                .depthFirst()
                .uniqueness(uniquenessType)
                .evaluator(timeValueEvaluator)
                .relationships(Rels.ImpactEdge, Direction.OUTGOING)
                .traverse(startNode)) {


            Relationship lastRel = position.lastRelationship();

            ArrayList<String> alChildren;
            if (position.length() > 0) {
                Node toNode = lastRel.getEndNode();
                Node fromNode = lastRel.getStartNode();
                String fromName = (String) fromNode.getProperty("qName");
                String toName = (String) toNode.getProperty("qName");

                System.out.println(" fromName " + fromName + " to " + toName);

                if (hmNodes.containsKey(fromName)) {
                    alChildren = hmNodes.get(fromName);
                } else {
                    alChildren = new ArrayList<String>();
                }
                // load an empty arraylist for the to node so it doesn't get left out
                if (!hmNodes.containsKey(toName)) {
                    hmNodes.put(toName, new ArrayList<String>());
                }
                // to ensure that the children are unique
                if (!alChildren.contains(toName)) {
                    alChildren.add(toName);
                    hmNodes.put(fromName, alChildren);
                }
            } else {
                System.out.println(" skipping  rel " + position.length());
            }

        }

        System.out.println(" hmNodes " + new Gson().toJson(hmNodes));
        long end = System.currentTimeMillis();
        System.out.println(" number of parents  =  " + hmNodes.size());
        System.out.println(" traversal elapsed =  " + (end - start));


        return hmNodes.entrySet().stream().map(entry -> new KeyValueResult(entry.getKey(), entry.getValue()));

    }

    private enum Rels implements RelationshipType {
        ImpactEdge
    }

    public class KeyValueResult {
        public final Object key;
        public final Object value;

        public KeyValueResult(Object key, Object value) {
            this.key = key;
            this.value = value;
        }
    }


}
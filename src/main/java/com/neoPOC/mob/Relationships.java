package com.neoPOC.mob;


import org.neo4j.graphdb.RelationshipType;

public enum Relationships implements RelationshipType {
    LIKES, KNOWS, ImpactEdge
}
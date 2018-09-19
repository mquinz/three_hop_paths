# Three Hop Paths
Multiple origin multiple destination 3 relationships queries for knowledge graphs using Neo4j

This project requires Neo4j 3.4.x or higher

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/three-hop-paths-1.0-SNAPSHOT.jar neo4j-enterprise-3.4.7/plugins/.
    

Restart your Neo4j Server. A new Stored Procedure is available:


    CALL com.maxdemarzi.three_hop_paths([from], [to], {weights})
    
See http://maxdemarzi.com blog post on this for more details    
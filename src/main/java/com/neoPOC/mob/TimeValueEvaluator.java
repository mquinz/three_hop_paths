/**
 *
 */
package com.neoPOC.mob;


import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;


/**
 *  Simple evaluator to stop the traversal when it comes across a node Id
 *  contained in list of ids.  A treeset is used instead of an arraylist to speed lookups.
 *
 *
 * @author markquinsland
 *
 *
 */
public class TimeValueEvaluator implements Evaluator {
    String className = this.getClass().getSimpleName();
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    private Log log;
    private Long timeValue = 0l;
    private Boolean continueOnExclude = true;


    public TimeValueEvaluator(Long timeValue, Boolean continueOnExclude) {
        this.timeValue = timeValue;
        this.continueOnExclude = continueOnExclude;
    }

    public Evaluation evaluate(Path path) {


        try {
            Relationship lastRel = path.lastRelationship();
            if (lastRel == null) {
                //	 System.out.println ("  exclude null rel "  );
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }


            Long sTime = (Long) lastRel.getProperty("sTime");
            Long eTime = (Long) lastRel.getProperty("eTime");

            if ((sTime == null) || (eTime == null)) {
                //	System.out.println (className + " time is null exclude and continue ");
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }

            /*
             *  if the timevalue parm is between the start and end times of the relationship,
             *  include it and continue.
             *
             *  If not, then the relationship will be excluded,
             *  but a decision must be made about whether or not to continue the traversal
             *
             *  Discontinuing the traversal essentially means that any part of the path that
             *  fails to meet the filtering criteria terminates the entire traversal,
             *  This is the equivalent of a WHERE ALL () condition.
             *
             *  Continuing the traversal will return ALL relationships in the path that meet the criteria .
             *
             *
             */

            if ((sTime < timeValue) && (timeValue < eTime)) {
                //		System.out.println("valid time ");
                return Evaluation.INCLUDE_AND_CONTINUE;

            } else {
//                System.out.println("invalid time ");


                if (continueOnExclude) {

                    //					System.out.println (className + " EXCLUDE_AND_CONTINUE "  + lastRel );
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                } else {

                    //					System.out.println (className + " EXCLUDE_AND_PRUNE "  + lastRel );
                    return Evaluation.EXCLUDE_AND_PRUNE;
                }
            }


        } catch (Exception e) {

            e.printStackTrace();
            log.error(className + " exclude and prune due to exception ");
            return Evaluation.EXCLUDE_AND_PRUNE;

        }
    }
}
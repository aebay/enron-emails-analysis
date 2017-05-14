package org.uk.aeb.processors.logic;

import scala.Tuple2;

import java.util.Comparator;

/**
 * Created by AEB on 14/05/17.
 */
public class RecipientComparator implements Comparator<Tuple2< String, Long >> {

    @Override
    public int compare( Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) {
        return Long.compare(tuple1._2, tuple2._2);
    }

}

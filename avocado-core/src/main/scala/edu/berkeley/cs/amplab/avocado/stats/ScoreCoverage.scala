/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.avocado.stats

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

private[stats] object ScoreCoverage {

  // TODO: correct for whole genome without gaps, incorrect if gaps are present
  def apply(rdd: RDD[ADAMRecord]): Double = {
    
    def coverageReducer (t1: (Long, Long, Long), t2: (Long, Long, Long)): (Long, Long, Long) = {
      (t1._1 min t2._1,
       t1._2 max t2._1,
       t1._3 +   t2._3)
    }
    
    def readToParams (r: ADAMRecord): (Long, Long, Long) = {
      val s = r.getStart
      val e = r.end.get
      (s, e, e - s + 1)
    }
    
    val (start, end, bases) = rdd.filter(_.getReadMapped).map(readToParams)
      .reduce(coverageReducer(_, _))
    
    bases.toDouble / (end - start).toDouble
  }

}

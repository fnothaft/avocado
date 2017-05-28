/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.avocado.joint

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.variant.GenotypeRDD
import org.bdgenomics.formats.avro.{ Genotype, Variant }
import org.bdgenomics.utils.misc.Logging

/**
 * Uses an EM-based approach to update genotype calls using allele frequencies.
 */
object JointCaller extends Serializable with Logging {

  /**
   * Jointly calls variants across multiple samples.
   *
   * @param rdd An RDD of Genotype records, including both variant sites and
   *   reference models.
   * @param maxIterations The maximum number of iterations to run. Must be zero
   *   or positive. If zero, we only run initialization via maximum-likelihood.
   * @return Returns a squared off set of variant calls.
   */
  def jointCall(rdd: GenotypeRDD,
                maxIterations: Int = 0): GenotypeRDD = {

    if (rdd.samples.size == 1) {
      log.warn("Only saw a single sample. Skipping joint calling...")
      rdd
    } else {

      // generate squared variants
      val squared = SquareVariants(rdd)

      // initialize EM
      val initialEstimate = initialize(squared)

      // run EM
      val finalEstimate = iterate(initialEstimate, maxIterations)

      // finalize the genotype calls
      val finalCalls = finalize(finalEstimate)

      rdd.copy(rdd = finalCalls)
    }
  }

  /**
   * @param rdd Squared off variants along with genotypes.
   * @return Initializes the major allele frequency by estimating using a
   *   maximum likelihood estimator.
   */
  private[joint] def initialize(
    rdd: RDD[(Variant, Iterable[Genotype])]): RDD[((Variant, Double), Iterable[Genotype])] = {
    ???
  }

  /**
   * @param rdd Squared off variants, initial major allele frequency estimate,
   *   along with genotypes.
   * @param maxIterations The maximum number of iterations of EM to run.
   * @return Returns a final estimate of the allele frequency.
   */
  private[joint] def iterate(
    rdd: RDD[((Variant, Double), Iterable[Genotype])],
    maxIterations: Int): RDD[((Variant, Double), Iterable[Genotype])] = {
    ???
  }

  /**
   * Finalizes the genotype calls using the major allele frequency estimate.
   *
   * @param Squared off variants, initial major allele frequency estimate,
   *   along with genotypes.
   * @return Returns a final RDD of Genotypes where the genotype calls have
   *   been updated using the major allele frequency as a prior.
   */
  private[joint] def finalize(
    rdd: RDD[((Variant, Double), Iterable[Genotype])]): RDD[Genotype] = {
    ???
  }
}

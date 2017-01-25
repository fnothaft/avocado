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
package org.bdgenomics.avocado.genotyping

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.variant.{ GenotypeRDD, VariantRDD }
import org.bdgenomics.formats.avro.{
  Genotype,
  GenotypeAllele,
  Variant
}
import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
 * Genotypes samples by calculating a posterior across multiple samples.
 *
 * We calculate a minor allele frequency for each variant site. We normalize the
 * provided genotype likelihoods and then use the allele frequency as a prior
 * to calculate the posterior genotype probabilities. These posterior
 * probabilities are used to re-genotype each site. The minor allele frequency
 * is estimated by running expectation-maximization.
 */
private[avocado] object JointGenotyper extends Serializable {

  /**
   * Jointly calls variants across a set of genotypes with likelihoods.
   *
   * "Squares off" a set of variant calls across samples, given a set of
   * variants and per-sample/per-site genotype records. It is assumed that
   * each sample has at least one genotype record that covers each variant.
   * This is true if the genotype records are loaded from a properly
   * constructed gVCF.
   *
   * @param variants The variants to "square off".
   * @param genotypes Genotype records containing likelihoods.
   * @param callQuality The minimum phred scaled quality to consider a "valid"
   *   genotype call.
   * @param iterations The number of iterations of expectation-maximization to
   *   run.
   * @return Returns a squared off GenotypeRDD.
   */
  def jointCall(variants: VariantRDD,
                genotypes: GenotypeRDD,
                callQuality: Int = 30,
                iterations: Int = 10): GenotypeRDD = {

    @tailrec def iterateFrequencies(
      iteration: Int,
      frequencies: RDD[(Variant, Double)]): GenotypeRDD = {
      if (iteration == 0) {
        finalizeCalls(frequencies, genotypes, callQuality)
      } else {
        val estimatedGenotypes = estimateGenotypes(frequencies, genotypes)
        iterateFrequencies(iteration - 1,
          estimateAlleleFrequencies(estimatedGenotypes))
      }
    }

    // initialize allele frequencies
    val initialFrequencies = initializeSites(variants,
      genotypes.samples.map(_.getSampleId))

    iterateFrequencies(iterations, initialFrequencies)
  }

  /**
   * Discovers variant sites, which are then jointly called across samples.
   *
   * "Squares off" a set of variant calls across samples. First, variant
   * sites are identified by looking at "high confidence" genotype calls
   * in the input. These sites are filtered using the emitQuality. Then,
   * these sites are joint called across the genotypes. It is assumed that
   * each sample has at least one genotype record that covers each variant.
   * This is true if the genotype records are loaded from a properly
   * constructed gVCF.
   *
   * @param genotypes Genotype records containing likelihoods.
   * @param emitQuality The minimum phred scaled quality to consider a
   *   preliminary genotype as a variant site.
   * @param callQuality The minimum phred scaled quality to consider a "valid"
   *   genotype call.
   * @param iterations The number of iterations of expectation-maximization to
   *   run.
   * @return Returns a squared off GenotypeRDD.
   */
  def discoverAndJointCall(genotypes: GenotypeRDD,
                           emitQuality: Int = 10,
                           callQuality: Int = 30,
                           iterations: Int = 10): GenotypeRDD = {

    // filter out possibly variant sites
    val variantsToCall = filterSites(genotypes, emitQuality)

    // call variants
    jointCall(variantsToCall, genotypes,
      callQuality = callQuality,
      iterations = iterations)
  }

  private[genotyping] def filterSites(genotypes: GenotypeRDD,
                                      emitQuality: Int): VariantRDD = {
    VariantRDD(
      genotypes.rdd
        .flatMap(gt => {
          if (!gt.getAlleles.forall(a => a == GenotypeAllele.REF) &&
            gt.getGenotypeQuality > emitQuality) {
            Some(Variant.newBuilder
              .setStart(gt.getStart)
              .setEnd(gt.getEnd)
              .setReferenceAllele(gt.getVariant.getReferenceAllele)
              .setAlternateAllele(gt.getVariant.getAlternateAllele)
              .build)
          } else {
            None
          }
        }).distinct,
      genotypes.sequences)
  }

  private[genotyping] def initializeSites(
    variants: VariantRDD,
    samples: Seq[String]): RDD[(Variant, Double)] = {
    ???
  }

  private[genotyping] def estimateGenotypes(
    frequencies: RDD[(Variant, Double)],
    genotypes: GenotypeRDD): RDD[(Variant, Double)] = {
    ???
  }

  private[genotyping] def estimateGenotypeWithPrior(
    genotype: Genotype,
    variant: Variant,
    frequency: Double): (Variant, Double) = {
    ???
  }

  private[genotyping] def estimateAlleleFrequencies(
    estimatedGenotypes: RDD[(Variant, Double)]): RDD[(Variant, Double)] = {
    ???
  }

  private[genotyping] def finalizeCalls(
    frequencies: RDD[(Variant, Double)],
    genotypes: GenotypeRDD,
    callQuality: Int): GenotypeRDD = {
    ???
  }

  private[genotyping] def finalizeCall(
    genotype: Genotype,
    variant: Variant,
    frequency: Double): Genotype = {
    ???
  }
}

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

import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
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

    // extract loci to call
    val loci = sitesToLoci(variants)
      .cache

    // populate observed loci
    val observedLoci: RDD[(ReferenceRegion, ObservedLocus)] =
      observeLoci(loci, genotypes)
        .cache

    // initialize MAF estimates
    val initialFrequencies = initializeLoci(observedLoci)

    @tailrec def iterateFrequencies(
      iteration: Int,
      frequencies: RDD[(ReferenceRegion, Double)]): RDD[Genotype] = {
      if (iteration == 0) {
        finalizeCalls(frequencies,
          loci,
          observedLoci,
          callQuality)
      } else {
        iterateFrequencies(iteration - 1,
          estimateAlleleFrequencies(frequencies,
            observedLoci))
      }
    }

    // loop and estimate MAFs and fill in genotypes
    val jointGenotypes = iterateFrequencies(iterations,
      initialFrequencies)

    // unpersist loci
    loci.unpersist()
    observedLoci.unpersist()

    genotypes.copy(rdd = jointGenotypes)
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

  private[genotyping] def sitesToLoci(
    variants: VariantRDD): RDD[(ReferenceRegion, Locus)] = {
    variants.rdd
      .keyBy(v => ReferenceRegion(v))
      .groupByKey()
      .mapValues(v => Locus(v))
  }

  private[genotyping] def observeLoci(
    loci: RDD[(ReferenceRegion, Locus)],
    genotypes: GenotypeRDD): RDD[(ReferenceRegion, ObservedLocus)] = {

    // map of sample ids
    val samples = genotypes.samples.map(_.getSampleId)

    // fill in all loci, even if they haven't been observed
    val locusNoObs = loci.flatMap(p => samples.map(s => ((p._1, s), None)))

    // map all genotypes down
    val gtsByLocus = genotypes.rdd
      .groupBy(gt => (ReferenceRegion(gt), gt.getSampleId))

    // join together
    locusNoObs.leftOuterJoin(gtsByLocus)
      .map(p => {
        val ((locus, sample), (_, optGts)) = p

        optGts.fold((locus, ObservedLocus(sample)))(gts => {
          (locus, ObservedLocus(sample, genotypes = gts))
        })
      })
  }

  private[genotyping] def initializeLoci(
    loci: RDD[(ReferenceRegion, ObservedLocus)]): RDD[(ReferenceRegion, Double)] = {

    loci.mapValues(_.avgCall)
      .reduceByKey((p1, p2) => {
        (p1._1 + p2._1, p1._2 + p2._2)
      }).mapValues(p => {
        p._1 / p._2.toDouble
      })
  }

  private[genotyping] def estimateAlleleFrequencies(
    frequencies: RDD[(ReferenceRegion, Double)],
    loci: RDD[(ReferenceRegion, ObservedLocus)]): RDD[(ReferenceRegion, Double)] = {
    frequencies.join(loci)
      .map(kv => {
        val (rr, (maf, locus)) = kv
        (rr, locus.estimateRefCalls(maf))
      }).reduceByKey((p1, p2) => {
        (p1._1 + p2._1, p1._2 + p2._2)
      }).mapValues(p => {
        p._1 / p._2.toDouble
      })
  }

  private[genotyping] def finalizeCalls(
    frequencies: RDD[(ReferenceRegion, Double)],
    loci: RDD[(ReferenceRegion, Locus)],
    observedLoci: RDD[(ReferenceRegion, ObservedLocus)],
    callQuality: Int): RDD[Genotype] = {
    frequencies.join(loci)
      .join(observedLoci)
      .flatMap(kv => {
        val (_, ((maf, locus), observedLocus)) = kv
        observedLocus.finalize(maf, callQuality, locus.variants)
      })
  }
}

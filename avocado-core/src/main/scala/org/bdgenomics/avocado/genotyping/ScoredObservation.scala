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

import org.apache.spark.SparkContext
import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  SQLContext
}
import org.apache.spark.sql.functions._
import org.bdgenomics.adam.util.PhredUtils

/**
 * Companion object for creating scored observations.
 */
private[genotyping] object ScoredObservation extends Serializable {

  /**
   * Creates a scored observation given strand and quality info.
   *
   * @param isRef Is this the reference allele?
   * @param isOther Is this an other-alt allele?
   * @param forwardStrand Was this on the forward strand?
   * @param optQuality What is the quality of this observation, if defined?
   * @param mapQ What is the mapping quality of the read this is from?
   * @param ploidy How many copies of chromosomes exist at this site?
   * @return Returns a scored observation.
   */
  def apply(isRef: Boolean,
            isOther: Boolean,
            forwardStrand: Boolean,
            optQuality: Option[Int],
            mapQ: Int,
            ploidy: Int): ScoredObservation = {
    val mapSuccessProbability = PhredUtils.phredToSuccessProbability(mapQ)
    val (altLikelihoods, nonRefLikelihoods) = Observer.likelihoods(
      ploidy,
      mapSuccessProbability,
      optQuality)

    val zeros: Array[Double] = null
    val (referenceLikelihoods, alleleLikelihoods, otherLikelihoods) = if (isOther) {
      (zeros, zeros, altLikelihoods)
    } else {
      if (isRef) {
        (altLikelihoods, zeros, zeros)
      } else {
        (zeros, altLikelihoods, zeros)
      }
    }

    ScoredObservation(isRef,
      isOther,
      forwardStrand,
      optQuality,
      mapQ,
      if (!isRef && !isOther && forwardStrand) 1 else 0,
      if (isRef && forwardStrand) 1 else 0,
      (mapQ * mapQ).toDouble,
      referenceLikelihoods,
      alleleLikelihoods,
      otherLikelihoods,
      if (!isRef && !isOther) 1 else 0,
      if (isRef) 1 else 0,
      1)
  }

  /**
   * Builds a table of scored observations.
   *
   * @param sc A SparkContext to use to access the Spark SQL APIs.
   * @param maxQuality The highest base quality score to allow.
   * @param maxMapQ The highest mapping quality score to allow.
   * @param ploidy The number of chromosomes to build in the table.
   * @return Returns a Dataset of ScoredObservations.
   */
  def createScores(sc: SparkContext,
                   maxQuality: Int,
                   maxMapQ: Int,
                   ploidy: Int): Dataset[ScoredObservation] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.createDataset(
      (Seq(None.asInstanceOf[Option[Int]]) ++
        (0 to maxQuality).map(q => Some(q))).flatMap(optQ => {
          (0 to maxMapQ).flatMap(mq => {
            Seq(
              ScoredObservation(true, true, true,
                optQ, mq,
                ploidy),
              ScoredObservation(false, true, true,
                optQ, mq,
                ploidy),
              ScoredObservation(true, false, true,
                optQ, mq,
                ploidy),
              ScoredObservation(false, false, true,
                optQ, mq,
                ploidy),
              ScoredObservation(true, true, false,
                optQ, mq,
                ploidy),
              ScoredObservation(false, true, false,
                optQ, mq,
                ploidy),
              ScoredObservation(true, false, false,
                optQ, mq,
                ploidy),
              ScoredObservation(false, false, false,
                optQ, mq,
                ploidy))
          })
        }))
  }

  /**
   * Builds a table of scored observations, with a flat schema.
   *
   * @param sc A SparkContext to use to access the Spark SQL APIs.
   * @param maxQuality The highest base quality score to allow.
   * @param maxMapQ The highest mapping quality score to allow.
   * @param ploidy The number of chromosomes to build in the table.
   * @return Returns a DataFrame of ScoredObservations, with the arrays in the
   *   schema flattened.
   */
  def createFlattenedScores(sc: SparkContext,
                            maxQuality: Int,
                            maxMapQ: Int,
                            ploidy: Int): DataFrame = {
    val scoreDf = createScores(sc, maxQuality, maxMapQ, ploidy)
      .toDF

    scoreDf.select((
      Seq(scoreDf("isRef"),
        scoreDf("isOther"),
        scoreDf("forwardStrand"),
        scoreDf("optQuality"),
        scoreDf("mapQ"),
        scoreDf("alleleForwardStrand"),
        scoreDf("otherForwardStrand"),
        scoreDf("squareMapQ")) ++ (0 to ploidy).map(p => {
          when(scoreDf("referenceLogLikelihoods").isNotNull,
            scoreDf("referenceLogLikelihoods").getItem(p))
            .as("referenceLogLikelihoods%d".format(p))
        }) ++ (0 to ploidy).map(p => {
          when(scoreDf("alleleLogLikelihoods").isNotNull,
            scoreDf("alleleLogLikelihoods").getItem(p))
            .as("alleleLogLikelihoods%d".format(p))
        }) ++ (0 to ploidy).map(p => {
          when(scoreDf("otherLogLikelihoods").isNotNull,
            scoreDf("otherLogLikelihoods").getItem(p))
            .as("otherLogLikelihoods%d".format(p))
        }) ++ Seq(scoreDf("alleleCoverage"),
          scoreDf("otherCoverage"),
          scoreDf("totalCoverage"))): _*)
  }
}

/**
 * A precomputed, scored observation.
 *
 * @param isRef Is this a reference allele?
 * @param isOther Is this an other-alt allele?
 * @param forwardStrand Is this on the forward strand?
 * @param optQuality What is the base quality of this observation, if defined?
 * @param mapQ What is the mapping quality of this observation?
 * @param alleleForwardStrand The number of reads covering the allele observed
 *   on the forward strand.
 * @param otherForwardStrand The number of reads covering the site but not
 *   matching the allele observed on the forward strand.
 * @param squareMapQ The sum of the squares of the mapping qualities observed.
 * @param alleleLogLikelihoods The log likelihoods that 0...n copies of the
 *   reference allele were observed.
 * @param alleleLogLikelihoods The log likelihoods that 0...n copies of this
 *   allele were observed.
 * @param otherLogLikelihoods The log likelihoods that 0...n copies of another
 *   allele were observed.
 * @param alleleCoverage The total number of reads observed that cover the
 *   site and match the allele.
 * @param otherCoverage The total number of reads observed that cover the site
 *   but that do not match the allele.
 * @param totalCoverage The total number of reads that cover the site.
 */
private[genotyping] case class ScoredObservation(
    isRef: Boolean,
    isOther: Boolean,
    forwardStrand: Boolean,
    optQuality: Option[Int],
    mapQ: Int,
    alleleForwardStrand: Int,
    otherForwardStrand: Int,
    squareMapQ: Double,
    referenceLogLikelihoods: Array[Double],
    alleleLogLikelihoods: Array[Double],
    otherLogLikelihoods: Array[Double],
    alleleCoverage: Int,
    otherCoverage: Int,
    totalCoverage: Int) {
}
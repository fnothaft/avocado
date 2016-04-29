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

import org.apache.commons.configuration.{ HierarchicalConfiguration, SubnodeConfiguration }
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext, Row }
import org.apache.spark.sql.types._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.avocado.models.{ AlleleObservation, Observation }
import org.bdgenomics.avocado.stats.AvocadoConfigAndStats

object BiallelicDatasetGenotyper extends GenotyperCompanion {

  val genotyperName: String = "BiallelicDatasetGenotyper"

  protected def apply(stats: AvocadoConfigAndStats,
                      config: SubnodeConfiguration): Genotyper = {

    new BiallelicDatasetGenotyper(config.getInt("ploidy", 2))
  }
}

class BiallelicDatasetGenotyper(ploidy: Int) extends Genotyper with Logging {

  val companion: GenotyperCompanion = BiallelicDatasetGenotyper

  def genotype(observations: RDD[Observation]): RDD[VariantContext] = {

    // create sql context
    val sqlContext = new SQLContext(observations.context)

    // create schema
    val schema = StructType(Seq(
      StructField("reference", StringType, false),
      StructField("sample", StringType, false),
      StructField("position", LongType, false),
      StructField("allele", StringType, false),
      StructField("quality", IntegerType, false)))

    // convert rdd to dataframe
    val rowRdd = observations.map(obs => obs match {
      case ao: AlleleObservation => {
        Row(ao.pos.referenceName,
          ao.sample,
          ao.pos.pos,
          ao.allele,
          ao.mapq.fold(ao.phred)(p => (ao.phred + p) / 2))
      }
      case obs: Observation => {
        Row(obs.pos.referenceName,
          obs.pos.referenceName,
          obs.pos.pos,
          obs.allele,
          0)
      }
    })
    val obsDf = sqlContext.createDataFrame(rowRdd, schema)

    // compute likelihoods
    val likelihoodDataframe = generateLikelihoods(obsDf)

    // joint genotype
    val genotypedData = genotypeSamples(likelihoodDataframe)

    // convert dataframe into genotype RDD
    generateGenotypes(genotypedData)
  }

  def generateLikelihoods(obsDf: DataFrame): DataFrame = {
    ???
  }

  def genotypeSamples(likelihoodDf: DataFrame): DataFrame = {
    ???
  }

  def generateGenotypes(genotypeDf: DataFrame): RDD[VariantContext] = {
    ???
  }
}

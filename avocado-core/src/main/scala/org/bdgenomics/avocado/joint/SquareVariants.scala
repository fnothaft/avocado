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
import org.bdgenomics.adam.rdd.variant.{ GenotypeRDD, VariantRDD }
import org.bdgenomics.formats.avro.{ Genotype, Variant }

/**
 * Identifies variant sites and groups genotypes by those sites.
 */
private[joint] object SquareVariants extends Serializable {

  /**
   * Builds a squared set of genotypes.
   *
   * Identifies variant sites, and then groups together all evidence overlapping
   * those variants.
   *
   * @param rdd An RDD of Genotype records, including both variant sites and
   *   reference models.
   * @return Returns an RDD containing all the variants, along with all the
   *   genotype records that overlap those variants.
   */
  def apply(rdd: GenotypeRDD): RDD[(Variant, Iterable[Genotype])] = {
    ???
  }

  /**
   * Identifies sites that are variant.
   *
   * @param rdd An RDD of Genotype records, including both variant sites and
   *   reference models.
   * @return Returns an RDD containing the variant sites in our dataset.
   */
  def identify(rdd: GenotypeRDD): VariantRDD = {
    ???
  }
}

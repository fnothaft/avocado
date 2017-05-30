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

import org.bdgenomics.formats.avro.Variant

/**
 * Companion object for creating IndexedVariants.
 */
private[genotyping] object IndexedVariant {

  /**
   * @param variant The variant to convert.
   * @param contigIdx The index of the contig the variant is on.
   * @return Returns a case class-based representation of the variant.
   */
  def apply(variant: Variant,
            contigIdx: Int): IndexedVariant = {
    new IndexedVariant(contigIdx,
      variant.getStart.toInt,
      variant.getReferenceAllele,
      variant.getAlternateAllele)
  }
}

case class IndexedVariant(
    contigIdx: Int,
    start: Int,
    referenceAllele: String,
    alternateAllele: String) {

  /**
   * @param contigNames A seq containing the known contig names.
   * @return Returns an avro representation of this variant.
   */
  def toVariant(contigNames: Seq[String]): Variant = {
    Variant.newBuilder
      .setContigName(contigNames(contigIdx))
      .setStart(start.toLong)
      .setEnd((start + referenceAllele.length).toLong)
      .setReferenceAllele(referenceAllele)
      .setAlternateAllele(alternateAllele)
      .build
  }
}

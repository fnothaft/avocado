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

package edu.berkeley.cs.amplab.avocado.postprocessing

import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, ADAMGenotype, ADAMVariant}
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.avocado.stats.AvocadoConfigAndStats
import org.scalatest.FunSuite 

class FilterDepthSuite extends FunSuite {

  test("do not filter genotypes that have no depth info") {
    val contig = ADAMContig.newBuilder
        .setContigId(0)
        .setContigName("0")
        .build
    val variant = ADAMVariant.newBuilder
      .setContig(contig)
      .setPosition(0)
      .setReferenceAllele("A")
      .setVariantAllele("C")
      .build
    val gt1 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .build()

    val seq = Seq(gt1, gt1, gt1)
    val filt = new DepthFilter(10)

    assert(filt.filterGenotypes(seq).length === 3)
  }

  test("do not filter genotypes that have sufficient coverage") {
    val contig = ADAMContig.newBuilder
        .setContigId(0)
        .setContigName("0")
        .build
    val variant = ADAMVariant.newBuilder
      .setContig(contig)
      .setPosition(0)
      .setReferenceAllele("A")
      .setVariantAllele("C")
      .build
    val gt1 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .setReadDepth(10)
      .build()

    val seq = Seq(gt1, gt1)
    val filt = new DepthFilter(10)

    assert(filt.filterGenotypes(seq).length === 2)
  }

  test("filter genotypes that have low coverage") {
    val contig = ADAMContig.newBuilder
        .setContigId(0)
        .setContigName("0")
        .build
    val variant = ADAMVariant.newBuilder
      .setContig(contig)
      .setPosition(0)
      .setReferenceAllele("A")
      .setVariantAllele("C")
      .build
    val gt1 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .setReadDepth(5)
      .build()

    val seq = Seq(gt1, gt1)
    val filt = new DepthFilter(10)

    assert(filt.filterGenotypes(seq).length === 0)
  }

  test("do not filter genotypes that have no depth info or that have sufficient coverage") {
    val contig = ADAMContig.newBuilder
        .setContigId(0)
        .setContigName("0")
        .build
    val variant = ADAMVariant.newBuilder
      .setContig(contig)
      .setPosition(0)
      .setReferenceAllele("A")
      .setVariantAllele("C")
      .build
    val gt1 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .setSampleId("me")
      .build()
    val gt2 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .setSampleId("you")
      .setReadDepth(15)
      .build()

    val seq = Seq(gt1, gt1, gt2, gt2)
    val filt = new DepthFilter(10)

    assert(filt.filterGenotypes(seq).length === 4)
  }

  test("do not filter genotypes that have no depth info but filter low coverage calls") {
    val contig = ADAMContig.newBuilder
        .setContigId(0)
        .setContigName("0")
        .build
    val variant = ADAMVariant.newBuilder
      .setContig(contig)
      .setPosition(0)
      .setReferenceAllele("A")
      .setVariantAllele("C")
      .build
    val gt1 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .setSampleId("me")
      .build()
    val gt2 = ADAMGenotype.newBuilder
      .setVariant(variant)
      .setSampleId("you")
      .setReadDepth(6)
      .build()

    val seq = Seq(gt1, gt1, gt2, gt2)
    val filt = new DepthFilter(10)

    assert(filt.filterGenotypes(seq).length === 2)
    assert(filt.filterGenotypes(seq).filter(_.getSampleId == "me").length === 2)
    assert(filt.filterGenotypes(seq).filter(_.getSampleId == "you").length === 0)
  }
  }


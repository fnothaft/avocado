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
package org.bdgenomics.avocado.util

import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.read.{ AlignedReadRDD, AlignmentRecordRDD }
import org.bdgenomics.avocado.AvocadoFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class LowQualityMaskerSuite extends AvocadoFunSuite {

  def readRdd(reads: Seq[AlignmentRecord]): AlignmentRecordRDD = {
    AlignedReadRDD(sc.parallelize(reads),
      SequenceDictionary.empty,
      RecordGroupDictionary.empty)
  }

  sparkTest("phred score must be within bounds") {
    intercept[IllegalArgumentException] {
      LowQualityMasker.maskReads(readRdd(Seq.empty), -1)
    }
    intercept[IllegalArgumentException] {
      LowQualityMasker.maskReads(readRdd(Seq.empty), 400)
    }
  }

  val mixedRead = AlignmentRecord.newBuilder
    .setReadName("mixed")
    .setSequence("AAAAAAAA")
    .setQual(Seq(0, 10, 35, 50, 45, 40, 20, 15).map(v => (v + 33.toChar)).mkString)
    .build

  val goodRead = AlignmentRecord.newBuilder
    .setReadName("good")
    .setSequence("AAAAAA")
    .setQual((40 + 33).toChar.toString * 6)
    .build

  val badRead = AlignmentRecord.newBuilder
    .setSequence("AAAAAA")
    .setQual((33).toChar.toString * 6)
    .build

  ignore("don't mask a read with all high quality bases") {
    val newRead = LowQualityMasker.maskRead(goodRead,
      (20 + 33).toChar)

    assert(newRead.getSequence === "AAAAAA")
  }

  ignore("mask all bases in a read with all low quality bases") {
    val newRead = LowQualityMasker.maskRead(badRead,
      (20 + 33).toChar)

    assert(newRead.getSequence === "NNNNNN")
  }

  ignore("mask bases in a read with mixed qualities") {
    val newRead = LowQualityMasker.maskRead(mixedRead,
      (20 + 33).toChar)

    assert(newRead.getSequence === "NNAAAAAN")
  }

  ignore("mask an rdd of reads") {
    val maskedReads = LowQualityMasker.maskReads(readRdd(Seq(goodRead,
      badRead,
      mixedRead)), 10)
      .rdd
      .collect
    assert(maskedReads.length === 3)
    val mixed = maskedReads.filter(_.getReadName === "mixed").head
    assert(mixed.getSequence === "NAAAAAAA")
    val good = maskedReads.filter(_.getReadName === "good").head
    assert(good.getSequence === "AAAAAA")
    val bad = maskedReads.filter(_.getReadName === "bad").head
    assert(bad.getSequence === "NNNNNN")
  }
}

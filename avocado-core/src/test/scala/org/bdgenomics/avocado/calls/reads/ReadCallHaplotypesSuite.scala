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
package org.bdgenomics.avocado.calls.reads

import net.sf.samtools.{ Cigar, CigarElement, CigarOperator, TextCigarCodec }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ GenotypeAllele, AlignmentRecord, Contig }
import org.bdgenomics.avocado.algorithms.hmm._
import org.bdgenomics.avocado.partitioners.PartitionSet
import parquet.filter.UnboundRecordFilter
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

trait ReadCallHaplotypesSuite extends SparkFunSuite {

  val emptyPartition = new PartitionSet(SortedMap[ReferenceRegion, Int]())
  val rc_short: ReadCallHaplotypes
  val rc_long: ReadCallHaplotypes
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  def na12878_chr20_snp_reads: RDD[RichAlignmentRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("NA12878_snp_A2G_chr20_225058.sam").getFile
    val rdd: RDD[AlignmentRecord] = sc.adamLoad(path)
    rdd.map(r => RichAlignmentRecord(r))
  }

  def make_read(sequence: String,
                start: Long,
                cigar: String,
                mdtag: String,
                length: Int,
                qualities: Seq[Int],
                id: Int = 0): RichAlignmentRecord = {

    val c = CIGAR_CODEC.decode(cigar)
    val end = c.getCigarElements
      .asScala
      .filter((p: CigarElement) => p.getOperator.consumesReferenceBases())
      .foldLeft(start - 1) {
        (pos, cigarEl) => pos + cigarEl.getLength
      }

    val contig = Contig.newBuilder()
      .setContigName("chr1")
      .build()

    RichAlignmentRecord(AlignmentRecord.newBuilder()
      .setReadName("read" + id.toString)
      .setStart(start)
      .setEnd(end)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setQual(qualities.map(_.toChar.toString).reduce(_ + _))
      .setMismatchingPositions(mdtag)
      .setRecordGroupSample("sample")
      .setContig(contig)
      .build())
  }

  test("\"Call\" hom ref, ~10x coverage") {
    val reference = "TACCAATGTAA"
    val read0 = make_read("TACCAAT", 0L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 0)
    val read1 = make_read("ACCAATG", 1L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 1)
    val read2 = make_read("CCAATGT", 2L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 2)
    val read3 = make_read("CAATGTA", 3L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 3)
    val read4 = make_read("AATGTAA", 4L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 4)
    val read5 = make_read("TACCAAT", 0L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 5)
    val read6 = make_read("ACCAATG", 1L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 6)
    val read7 = make_read("CCAATGT", 2L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 7)
    val read8 = make_read("CAATGTA", 3L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 8)
    val read9 = make_read("AATGTAA", 4L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 9)

    val readBucket = Seq(read0, read1, read2, read3, read4, read5, read6, read7, read8, read9)
    val kmerGraph = rc_short.generateHaplotypes(readBucket, reference)

    val variants = rc_short.scoreHaplotypes(readBucket, kmerGraph, reference)

    assert(variants.length === 0)
  }

  test("Call simple het SNP, ~10x coverage") {
    val reference = "TACCAATGTAA"
    val read0 = make_read("TACCCAT", 0L, "7M", "4A2", 7, Seq(50, 50, 50, 50, 50, 50, 50), 0)
    val read1 = make_read("ACCCATG", 1L, "7M", "3A3", 7, Seq(50, 50, 50, 50, 50, 50, 50), 1)
    val read2 = make_read("CCCATGT", 2L, "7M", "2A4", 7, Seq(50, 50, 50, 50, 50, 50, 50), 2)
    val read3 = make_read("CCATGTA", 3L, "7M", "1A5", 7, Seq(50, 50, 50, 50, 50, 50, 50), 3)
    val read4 = make_read("CATGTAA", 4L, "7M", "0A6", 7, Seq(50, 50, 50, 50, 50, 50, 50), 4)
    val read5 = make_read("TACCAAT", 0L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 5)
    val read6 = make_read("ACCAATG", 1L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 6)
    val read7 = make_read("CCAATGT", 2L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 7)
    val read8 = make_read("CAATGTA", 3L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 8)
    val read9 = make_read("AATGTAA", 4L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 9)

    val readBucket = Seq(read0, read1, read2, read3, read4, read5, read6, read7, read8)
    val kmerGraph = rc_short.generateHaplotypes(readBucket, reference)

    val variants = rc_short.scoreHaplotypes(readBucket, kmerGraph, reference)

    assert(variants.length === 1)
    assert(variants.head.position.pos === 4L)
    assert(variants.head.variant.variant.getReferenceAllele === "A")
    assert(variants.head.variant.variant.getAlternateAllele === "C")
    val alleles: List[GenotypeAllele] = asScalaBuffer(variants.head.genotypes.head.getAlleles).toList
    assert(alleles.length === 2)
    assert(alleles.head === GenotypeAllele.Ref)
    assert(alleles.last === GenotypeAllele.Alt)
  }

  test("Call simple hom SNP, ~10x coverage") {
    val reference = "TACCAATGTAA"
    val read0 = make_read("TACCCAT", 0L, "7M", "4A2", 7, Seq(50, 50, 50, 50, 50, 50, 50), 0)
    val read1 = make_read("ACCCATG", 1L, "7M", "3A3", 7, Seq(50, 50, 50, 50, 50, 50, 50), 1)
    val read2 = make_read("CCCATGT", 2L, "7M", "2A4", 7, Seq(50, 50, 50, 50, 50, 50, 50), 2)
    val read3 = make_read("CCATGTA", 3L, "7M", "1A5", 7, Seq(50, 50, 50, 50, 50, 50, 50), 3)
    val read4 = make_read("CATGTAA", 4L, "7M", "0A6", 7, Seq(50, 50, 50, 50, 50, 50, 50), 4)
    val read5 = make_read("TACCCAT", 0L, "7M", "4A2", 7, Seq(50, 50, 50, 50, 50, 50, 50), 5)
    val read6 = make_read("ACCCATG", 1L, "7M", "3A3", 7, Seq(50, 50, 50, 50, 50, 50, 50), 6)
    val read7 = make_read("CCCATGT", 2L, "7M", "2A4", 7, Seq(50, 50, 50, 50, 50, 50, 50), 7)
    val read8 = make_read("CCATGTA", 3L, "7M", "1A5", 7, Seq(50, 50, 50, 50, 50, 50, 50), 8)
    val read9 = make_read("CATGTAA", 4L, "7M", "0A6", 7, Seq(50, 50, 50, 50, 50, 50, 50), 9)

    val readBucket = Seq(read0, read1, read2, read3, read4, read5, read6, read7, read8, read9)
    val kmerGraph = rc_short.generateHaplotypes(readBucket, reference)

    val variants = rc_short.scoreHaplotypes(readBucket, kmerGraph, reference)

    assert(variants.length === 1)
    assert(variants.head.position.pos === 4L)
    assert(variants.head.variant.variant.getReferenceAllele === "A")
    assert(variants.head.variant.variant.getAlternateAllele === "C")
    val alleles: List[GenotypeAllele] = asScalaBuffer(variants.head.genotypes.head.getAlleles).toList
    assert(alleles.length === 2)
    assert(alleles.head === GenotypeAllele.Alt)
    assert(alleles.last === GenotypeAllele.Alt)
  }

  test("Call simple het INS, ~10x coverage") {
    val reference = "TACCAATGTAA"
    val read0 = make_read("TACCAAA", 0L, "4M1I2M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 0)
    val read1 = make_read("ACCAAAT", 1L, "3M1I3M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 1)
    val read2 = make_read("CCAAATG", 2L, "2M1I4M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 2)
    val read3 = make_read("CAAATGT", 2L, "1M1I5M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 3)
    val read4 = make_read("AAATGTA", 3L, "1I6M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 4)
    val read5 = make_read("TACCAAT", 0L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 5)
    val read6 = make_read("ACCAATG", 1L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 6)
    val read7 = make_read("CCAATGT", 2L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 7)
    val read8 = make_read("CAATGTA", 3L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 8)
    val read9 = make_read("AATGTAA", 4L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 9)

    val readBucket = Seq(read0, read1, read2, read3, read4, read5, read6, read7, read8, read9)
    val kmerGraph = rc_short.generateHaplotypes(readBucket, reference)

    val variants = rc_short.scoreHaplotypes(readBucket, kmerGraph, reference)

    assert(variants.length === 1)
    assert(variants.head.position.pos === 4L)
    assert(variants.head.variant.variant.getReferenceAllele === "A")
    assert(variants.head.variant.variant.getAlternateAllele === "AA")
    val alleles: List[GenotypeAllele] = asScalaBuffer(variants.head.genotypes.head.getAlleles).toList
    assert(alleles.length === 2)
    assert(alleles.head === GenotypeAllele.Ref)
    assert(alleles.last === GenotypeAllele.Alt)
  }

  test("Call simple het DEL, ~10x coverage") {
    val reference = "TACCAATGTAA"
    val read0 = make_read("TACCATG", 0L, "4M1D3M", "4^A3", 7, Seq(50, 50, 50, 50, 50, 50, 50), 0)
    val read1 = make_read("ACCATGT", 1L, "3M1D4M", "3^A4", 7, Seq(50, 50, 50, 50, 50, 50, 50), 1)
    val read2 = make_read("CCATGTA", 2L, "2M1D5M", "2^A5", 7, Seq(50, 50, 50, 50, 50, 50, 50), 2)
    val read3 = make_read("CATGTAA", 3L, "1M1D6M", "1^A6", 7, Seq(50, 50, 50, 50, 50, 50, 50), 3)
    val read4 = make_read("TACCAAT", 0L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 4)
    val read5 = make_read("ACCAATG", 1L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 5)
    val read6 = make_read("CCAATGT", 2L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 6)
    val read7 = make_read("CCAATGT", 2L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 7)
    val read8 = make_read("CAATGTA", 3L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 8)
    val read9 = make_read("AATGTAA", 4L, "7M", "7", 7, Seq(50, 50, 50, 50, 50, 50, 50), 9)

    val readBucket = Seq(read0, read1, read2, read3, read4, read5, read6, read7, read8, read9)
    val kmerGraph = rc_short.generateHaplotypes(readBucket, reference)

    val variants = rc_short.scoreHaplotypes(readBucket, kmerGraph, reference)

    assert(variants.length === 1)
    assert(variants.head.position.pos === 4L)
    assert(variants.head.variant.variant.getReferenceAllele === "CA")
    assert(variants.head.variant.variant.getAlternateAllele === "C")
    val alleles: List[GenotypeAllele] = asScalaBuffer(variants.head.genotypes.head.getAlleles).toList
    assert(alleles.length === 2)
    assert(alleles.head === GenotypeAllele.Ref)
    assert(alleles.last === GenotypeAllele.Alt)
  }

  sparkTest("call A->G snp on NA12878 chr20 @ 225058") {
    val reads = na12878_chr20_snp_reads.collect.toSeq
    val reference = rc_long.getReference(reads)

    val kmerGraph = rc_long.generateHaplotypes(reads, reference)

    val variants = rc_long.scoreHaplotypes(reads, kmerGraph, reference, Some(ReferenceRegion("chr20",
      225000L,
      225100L)))

    assert(variants.length === 1)
    assert(variants.head.position.pos === 225057L)
    assert(variants.head.variant.variant.getReferenceAllele === "A")
    assert(variants.head.variant.variant.getAlternateAllele === "G")
    val alleles: List[GenotypeAllele] = asScalaBuffer(variants.head.genotypes.head.getAlleles).toList
    assert(alleles.length === 2)
    assert(alleles.head === GenotypeAllele.Ref)
    assert(alleles.last === GenotypeAllele.Alt)
  }
}
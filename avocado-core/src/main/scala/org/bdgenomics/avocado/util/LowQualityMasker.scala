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

import java.lang.StringBuilder
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.annotation.tailrec

/**
 * Singleton object for masking bases with low quality scores to 'N'.
 *
 * These bases are then trimmed out when we run realignment and do not create
 * spurious bubbles.
 */
object LowQualityMasker extends Serializable {

  /**
   * Masks all low quality read bases as an N.
   *
   * @param reads The reads to mask low quality bases from.
   * @param quality The minimum Phred quality score to treat as a high quality
   *   base.
   * @return Returns all reads, but where low quality bases have been masked.
   */
  def maskReads(reads: AlignmentRecordRDD,
                quality: Int): AlignmentRecordRDD = {
    require(quality >= 0 && quality <= 255,
      "Quality threshold (%d) must be between 0 and 255, inclusive.".format(quality))

    // convert quality score to 33 based char
    val phredChar = (33 + quality).toChar

    reads.transform(rdd => {
      rdd.map(maskRead(_, phredChar))
    })
  }

  /**
   * @param read A single read to mask low quality bases from.
   * @param quality A Phred-33 based character that indicates the lowest phred
   *   score considered to be high quality.
   * @return Returns this read, with low quality bases masked.
   */
  private[util] def maskRead(read: AlignmentRecord,
                             quality: Char): AlignmentRecord = {
    require(read.getQual != null && read.getSequence != null,
      "Read quality and sequence must not be null: %s".format(read))
    val quals = read.getQual
    val sequence = read.getSequence
    val sequenceLength = sequence.length
    require(quals.length == sequenceLength,
      "Read quality and sequence have different lengths: %s".format(read))

    @tailrec def mask(sb: StringBuilder,
                      idx: Int = 0): String = {
      if (idx >= sequenceLength) {
        sb.toString
      } else {
        if (quals(idx) < quality) {
          sb.append('N')
        } else {
          sb.append(sequence(idx))
        }
        mask(sb, idx = idx + 1)
      }
    }

    // are any quality scores lower than our minimum quality?
    // if so, mask
    if (quals.exists(_ < quality)) {
      val maskedSequence = mask(new StringBuilder(sequenceLength))
      AlignmentRecord.newBuilder(read)
        .setSequence(maskedSequence)
        .build
    } else {
      read
    }
  }
}

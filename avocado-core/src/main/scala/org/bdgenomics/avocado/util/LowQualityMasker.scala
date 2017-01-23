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

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord

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

    ???
  }

  /**
   * @param read A single read to mask low quality bases from.
   * @param quality A Phred-33 based character that indicates the lowest phred
   *   score considered to be high quality.
   * @return Returns this read, with low quality bases masked.
   */
  private[util] def maskRead(read: AlignmentRecord,
                             quality: Char): AlignmentRecord = {
    ???
  }
}

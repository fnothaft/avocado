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

import org.bdgenomics.avocado.AvocadoFunSuite
import org.bdgenomics.formats.avro.{
  Genotype,
  GenotypeAllele,
  Variant
}
import scala.collection.JavaConversions._

class JointGenotyperSuite extends AvocadoFunSuite {

  ignore("extract no variants from all hom ref observations") {
    ???
  }

  ignore("extract no variants from low confidence alt observations") {
    ???
  }

  ignore("extract variants from a single sample dataset") {
    ???
  }

  ignore("extract variants from a dataset with multiple samples, no overlapping variants") {
    ???
  }

  ignore("extract variants from a dataset with multiple samples with overlapping variants") {
    ???
  }

  ignore("initializing sites works") {
    ???
  }

  ignore("run EM on a single site with very high confidence hom ref likelihoods") {
    ???
  }

  ignore("run EM on a single site with very high confidence het likelihoods") {
    ???
  }

  ignore("run EM on a single site with very high confidence hom alt likelihoods") {
    ???
  }

  ignore("run EM on a single site with mixed likelihoods, AF = 0.5") {
    ???
  }

  ignore("run EM on a single site with mixed likelihoods, AF = 0.75") {
    ???
  }

  ignore("run EM on a single site with mixed likelihoods, AF = 0.1") {
    ???
  }

  ignore("finalize a hom ref genotype for a single sample") {
    ???
  }

  ignore("finalize a het genotype for a single sample") {
    ???
  }

  ignore("finalize a hom alt genotype for a single sample") {
    ???
  }

  ignore("finalize multiple samples at a single site") {
    ???
  }

  ignore("finalize multiple samples and multiple sites") {
    ???
  }
}

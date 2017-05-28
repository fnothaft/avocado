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
package org.bdgenomics.avocado.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.avocado.joint.JointCaller
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object JointCall extends BDGCommandCompanion {
  val commandName = "jointCall"
  val commandDescription = "Call variants under a biallelic model"

  def apply(cmdLine: Array[String]) = {
    new JointCall(Args4j[JointCallArgs](cmdLine))
  }
}

class JointCallArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true,
    metaVar = "INPUT",
    usage = "The ADAM, BAM or SAM file to call",
    index = 0)
  var inputPath: String = null
  @Argument(required = true,
    metaVar = "OUTPUT",
    usage = "Location to write the variant output",
    index = 1)
  var outputPath: String = null
  @Args4jOption(required = false,
    name = "-max_iterations",
    usage = "Max number of iterations to run. Defaults to 0 (initialize via MLE only).")
  var maxIterations: Int = 0

  // required by ADAMSaveAnyArgs
  var sortFastqOutput: Boolean = false
  var asSingleFile: Boolean = false
  var deferMerging: Boolean = false
}

class JointCall(
    protected val args: JointCallArgs) extends BDGSparkCommand[JointCallArgs] {

  val companion = JointCall

  def run(sc: SparkContext) {

    // load in the input genotypes
    val rawGenotypes = sc.loadGenotypes(args.inputPath)

    // run joint calling
    val squaredOffGenotypes = JointCaller.jointCall(rawGenotypes,
      args.maxIterations)

    // save genotypes to disk
    squaredOffGenotypes.save(args)
  }
}

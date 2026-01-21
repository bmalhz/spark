/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.runtime

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.FileStatus


trait FileSorter[T] {
  val sortCriteria: FileStatus => T
  val ord: Ordering[T]

  def sortFiles(files: Seq[FileStatus], latestFirst: Boolean = false): Seq[FileStatus] = {
    if (latestFirst) {
      files.sortBy(sortCriteria)(ord.reverse)
    } else {
      files.sortBy(sortCriteria)(ord)
    }
  }
}

object FileSorter {
    def loadImpl(impl: String): FileSorter[_] = {
      import scala.reflect.runtime.currentMirror

      Try {
        val moduleSymbol = currentMirror.staticModule(impl)
        val moduleMirror = currentMirror.reflectModule(moduleSymbol)
        moduleMirror.instance
      } match {
        case Failure(exception) => throw new Exception(s"Invalid FileSorter implementation!")
        case Success(efs) => efs match {
          case fs: FileSorter[_] => fs
          case _ => throw new Exception(s"Provided Object ${impl} is not implementing FileSorter!")
        }
      }
    }
}

object ModificationTimeFileSorter extends FileSorter[Long] {
  override val ord = Ordering[Long]
  override val sortCriteria: FileStatus => Long = _.getModificationTime
}
object FilenameFileSorter extends FileSorter[String] {
  override val ord = Ordering[String]
  override val sortCriteria: FileStatus => String = _.getPath().toString()
}

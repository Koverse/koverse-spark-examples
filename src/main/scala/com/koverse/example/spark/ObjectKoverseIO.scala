/*
 * Copyright 2016 Koverse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.koverse.example.spark

import java.io._

object ObjectKoverseIO {

  //Reading Model from Koverse Record
  def objectFromBytes[A](modelArray:Array[Byte], classType: Class[A]): A = {
    val inputStream: ByteArrayInputStream = new ByteArrayInputStream(modelArray)
    val objectInput: ObjectInput = new ObjectInputStream(inputStream)
    objectInput.readObject().asInstanceOf[A]
  }

  //Prepping to store as Koverse Record
  def objectToBytes[A](model:A): Array[Byte] = {
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutput: ObjectOutput = new ObjectOutputStream(outputStream)
    objectOutput.writeObject(model.asInstanceOf[A])
    objectOutput.flush()
    outputStream.toByteArray
  }

}

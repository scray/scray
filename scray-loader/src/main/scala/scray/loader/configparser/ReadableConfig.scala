// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.loader.configparser

/**
 * A scray config object that has a method to read the config.
 * In principle this method can be executed any time, so this has
 * been designed as to be a reload.
 */
trait ReadableConfig[T <: ConfigProperties] { self =>
  
  /**
   * reads ConfigProperties from the main configuration object
   */
  def readConfig(config: ScrayConfiguration, old: T): Option[T]
}

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
 * Marker trait for proerties that can be checked for new properties being updates
 */
trait ConfigProperties {
  
  /**
   * check, if the new properties equal the original ones.
   * Returns true if the new properties are different from the original ones. False otherwise.
   */
  def needsUpdate(newProperties: Option[ConfigProperties]): Boolean =
    newProperties.map { _ != this }.getOrElse(true)
}

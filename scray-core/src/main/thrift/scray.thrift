/*
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
namespace java scray.service

exception ScrayException {
  1: i32 what,
  2: string why
}

struct ScrayResultFrame {
  1: string keyT,
  2: binary key,
  3: string rowT,
  4: binary row
}

service Scray extends shared.ScrayService {
	list<ScrayRow> query(1: string qString) throws (1: ScrayException ex)
}

/**
 *  Copyright 2016 SmartBear Software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.swagger.sample.data;

import io.swagger.sample.model.Category;
import io.swagger.sample.model.SyncObject;
import io.swagger.sample.model.Tag;

import java.util.List;
import java.util.ArrayList;

public class PetData {
  static List<SyncObject> pets = new ArrayList<SyncObject>();
  static List<Category> categories = new ArrayList<Category>();

  public SyncObject getPetById(String petId) {
    for (SyncObject pet : pets) {
    	System.out.println("Process sync point " + pet.getId() + "search " + petId);
      if (pet.getId().equals(petId)) {
        return pet;
      }
    }
    return null;
  }

  public void addPet(SyncObject pet) {
    if (pets.size() > 0) {
      for (int i = pets.size() - 1; i >= 0; i--) {
        if (pets.get(i).getId() == pet.getId()) {
          pets.remove(i);
        }
      }
    }
    
    System.out.println("Add sync point" + pet);
    pets.add(pet);
  }

  static SyncObject createPet(String id, String syncPoint, int version) {
    SyncObject pet = new SyncObject();
    pet.setId(id);
    pet.setSyncPoint(syncPoint);
    pet.setVersion(version);
    return pet;
  }
}
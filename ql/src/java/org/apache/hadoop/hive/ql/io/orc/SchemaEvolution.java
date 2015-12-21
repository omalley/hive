/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;
import org.apache.orc.TypeDescription;

/**
 * Take the file types and the (optional) configuration column names/types
 * and determine the mapping from the file schema to the reader schema.
 *
 * Valid promotions:
 *   BYTE -> SHORT -> INT -> LONG
 *   FLOAT -> DOUBLE
 *   new fields for STRUCTS and UNIONS at end
 */
class SchemaEvolution {

  // mapping from the reader column ids to the file column id
  private final int[] readerToFileColumnId;

  // the type of each reader column indexed by reader column id
  private final TypeDescription[] readerTypeArray;

  // the type of each file column indexed by reader column id
  private final TypeDescription[] fileTypeArray;

  /**
   * Build an identity map for readers that use the same type for both the
   * file and the reader.
   * @param schema the schema for both the file and the reader
   */
  SchemaEvolution(TypeDescription schema) {
    int numReaderTypes = schema.getMaximumId() + 1;
    readerToFileColumnId = new int[numReaderTypes];
    readerTypeArray = new TypeDescription[numReaderTypes];
    fileTypeArray = new TypeDescription[numReaderTypes];
    buildIdentity(schema);
  }

  void buildIdentity(TypeDescription schema) {
    int id = schema.getId();
    readerToFileColumnId[id] = id;
    readerTypeArray[id] = schema;
    fileTypeArray[id] = schema;
    for(TypeDescription child: schema.getChildren()) {
      buildIdentity(child);
    }
  }

  /**
   * Build a schema evolution for a pair of file and schema types
   * @param fileType the type found in the file
   * @param readerType the type desired by the reader
   * @throws IOException if the types are incompatible
   */
  SchemaEvolution(TypeDescription fileType,
                  TypeDescription readerType) throws IOException {
    int numReaderTypes = readerType.getMaximumId() + 1;
    readerToFileColumnId = new int[numReaderTypes];
    readerTypeArray = new TypeDescription[numReaderTypes];
    fileTypeArray = new TypeDescription[numReaderTypes];
    // map the reading types into the file types
    buildMapping(fileType, readerType);
  }

  /**
   * Build the mapping from the reader to the file schema.
   * @param fileType the type of the file
   * @param readerType the type that the reader wants
   * @throws IOException
   */
  void buildMapping(TypeDescription fileType,
                    TypeDescription readerType) throws IOException {
    int readerColumnId = readerType.getId();
    // check the validity
    if (fileType != null) {
      TypeDescription.Category readerCategory = readerType.getCategory();
      TypeDescription.Category fileCategory = fileType.getCategory();
      if (readerCategory != fileCategory) {
        boolean ok = true;
        switch (readerCategory) {
          case SHORT:
            if (fileCategory != TypeDescription.Category.BYTE) {
              ok = false;
            }
            break;
          case INT:
            if (fileCategory != TypeDescription.Category.BYTE &&
                fileCategory != TypeDescription.Category.SHORT) {
              ok = false;
            }
            break;
          case LONG:
            if (fileCategory != TypeDescription.Category.BYTE &&
                fileCategory != TypeDescription.Category.SHORT &&
                fileCategory != TypeDescription.Category.INT) {
              ok = false;
            }
            break;
          case DOUBLE:
            if (fileCategory != TypeDescription.Category.FLOAT) {
              ok = false;
            }
          default:
            ok = false;
            break;
        }
        if (!ok) {
          throw new IOException("ORC does not support type conversion from " +
              fileCategory.name() + " to " + readerCategory.name());
        }
      } else {
        switch (readerCategory) {
          case VARCHAR:
          case CHAR:
            if (fileType.getMaxLength() != readerType.getMaxLength()) {
              throw new IOException("ORC does not support type conversion from "
                  + fileType + " to " + readerType);
            }
            break;
          case DECIMAL:
            if (fileType.getScale() != readerType.getScale() ||
                fileType.getPrecision() != readerType.getPrecision()) {
              throw new IOException("ORC does not support type conversion from "
                  + fileType + " to " + readerType);
            }
            break;
          case STRUCT:
          case UNION: {
            List<TypeDescription> fileChildren = fileType.getChildren();
            List<TypeDescription> readerChildren = readerType.getChildren();
            if (fileChildren.size() > readerChildren.size()) {
              throw new IOException("ORC does not support type conversion" +
                  " from " + fileType + " to " + readerType);
            }
            for (int i = 0; i < fileChildren.size(); ++i) {
              buildMapping(fileChildren.get(i), readerChildren.get(i));
            }
            for (int i = fileChildren.size(); i < readerChildren.size(); ++i) {
              buildMapping(null, readerChildren.get(i));
            }
            break;
          }
          case MAP:
            buildMapping(fileType.getChildren().get(1),
                readerType.getChildren().get(1));
            // fall through
          case LIST:
            buildMapping(fileType.getChildren().get(0),
                readerType.getChildren().get(0));
            break;
          default:
            // the easy case of the same primitive types
            break;
        }
      }
    }
    // record the mapping
    readerToFileColumnId[readerColumnId] =
        fileType == null ? -1 : fileType.getId();
    readerTypeArray[readerColumnId] = readerType;
    fileTypeArray[readerColumnId] = fileType;
  }

  int getFileColumnId(int readerColumnId) {
    return readerToFileColumnId[readerColumnId];
  }

  TypeDescription getReaderType(int readerColumnId) {
    return readerTypeArray[readerColumnId];
  }

  TypeDescription getFileType(int readerColumnId) {
    return fileTypeArray[readerColumnId];
  }
}

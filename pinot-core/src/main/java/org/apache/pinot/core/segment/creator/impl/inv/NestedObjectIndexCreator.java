/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.creator.impl.inv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.spi.data.FieldSpec;


public class NestedObjectIndexCreator {

  public NestedObjectIndexCreator(File indexDir, FieldSpec fieldSpec, FieldSpec.DataType valueType) {

  }

  private static List<Map<String, String>> unnestJson(JsonNode root) {
    Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
    Map<String, String> flattenedSingleValuesMap = new TreeMap<>();
    Map<String, JsonNode> arrNodes = new TreeMap<>();
    Map<String, JsonNode> objectNodes = new TreeMap<>();
    List<Map<String, String>> resultList = new ArrayList<>();
    List<Map<String, String>> tempResultList = new ArrayList<>();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> child = fields.next();
      if (child.getValue().isValueNode()) {
        //Normal value node
        flattenedSingleValuesMap.put(child.getKey(), child.getValue().asText());
      } else if (child.getValue().isArray()) {
        //Array Node: Process these nodes later
        arrNodes.put(child.getKey(), child.getValue());
      } else {
        //Object Node
        objectNodes.put(child.getKey(), child.getValue());
      }
    }
    for (String objectNodeKey : objectNodes.keySet()) {
      JsonNode objectNode = objectNodes.get(objectNodeKey);
      modifyKeysInMap(flattenedSingleValuesMap, tempResultList, objectNodeKey, objectNode);
    }
    if (tempResultList.isEmpty()) {
      tempResultList.add(flattenedSingleValuesMap);
    }
    if (!arrNodes.isEmpty()) {
      for (Map<String, String> flattenedMapElement : tempResultList) {
        for (String arrNodeKey : arrNodes.keySet()) {
          JsonNode arrNode = arrNodes.get(arrNodeKey);
          for (JsonNode arrNodeElement : arrNode) {
            modifyKeysInMap(flattenedMapElement, resultList, arrNodeKey, arrNodeElement);
          }
        }
      }
    } else {
      resultList.addAll(tempResultList);
    }
    return resultList;
  }

  private static void modifyKeysInMap(Map<String, String> flattenedMap, List<Map<String, String>> resultList,
      String arrNodeKey, JsonNode arrNode) {
    List<Map<String, String>> objectResult = unnestJson(arrNode);
    for (Map<String, String> flattenedObject : objectResult) {
      Map<String, String> flattenedObjectCopy = new TreeMap<>(flattenedMap);
      for (Map.Entry<String, String> entry : flattenedObject.entrySet()) {
        flattenedObjectCopy.put(arrNodeKey + "." + entry.getKey(), entry.getValue());
      }
      resultList.add(flattenedObjectCopy);
    }
  }

  public void add(byte[] data)
      throws IOException {

    JsonNode jsonNode = new ObjectMapper().readTree(data);
    List<Map<String, String>> flattenedMapList = unnestJson(jsonNode);
    for (Map<String, String> map : flattenedMapList) {

    }
  }

  public static void main(String[] args)
      throws IOException {

    String json0 = " { \"a\" : { \"b\" : 1, \"c\": 3, \"d\": [{\"x\" : 1}, {\"y\" : 1}] }, \"e\": \"f\", \"g\":2.3 }";
    String json1 =
        " { \"name\" : \"adam\", \"age\": 30, \"country\": \"us\", \"address\": {\"number\" : 112, \"street\": \"main st\", \"country\": \"us\"  } }";
    String json2 = " { \"name\" : \"adam\", \"age\": 30 }";
    String json3 = "{\n" + "  \"name\" : \"adam\",\n" + "  \"age\" : 30,\n" + "  \"country\" : \"us\",\n"
        + "  \"addresses\" : [{\n" + "    \"number\" : 1,\n" + "    \"street\" : \"main st\",\n"
        + "    \"country\" : \"us\"\n" + "  }, {\n" + "    \"number\" : 2,\n" + "    \"street\" : \"second st\",\n"
        + "    \"country\" : \"us\"\n" + "  }, {\n" + "    \"number\" : 3,\n" + "    \"street\" : \"third st\",\n"
        + "    \"country\" : \"us\"\n" + "  }]\n" + "}\n";

    String json4 =
        "{\n" + "    \"year\": [\n" + "        2018\n" + "    ],\n" + "    \"customers\": [\n" + "        {\n"
            + "            \"name\": \"John\",\n" + "            \"contact\": [\n" + "                {\n"
            + "                    \"phone\": \"home\",\n" + "                    \"number\": \"333-3334\"\n"
            + "                }\n" + "            ]\n" + "        },\n" + "        {\n"
            + "            \"name\": \"Jane\",\n" + "            \"contact\": [\n" + "                {\n"
            + "                    \"phone\": \"home\",\n" + "                    \"number\": \"555-5556\"\n"
            + "                }\n" + "            ],\n" + "            \"surname\": \"Shaw\"\n" + "        }\n"
            + "    ]\n" + "}";

    String json5 = "{ \n" + "  \"accounting\" : [   \n" + "                     { \"firstName\" : \"John\",  \n"
        + "                       \"lastName\"  : \"Doe\",\n" + "                       \"age\"       : 23 },\n" + "\n"
        + "                     { \"firstName\" : \"Mary\",  \n" + "                       \"lastName\"  : \"Smith\",\n"
        + "                        \"age\"      : 32 }\n" + "                 ],                            \n"
        + "  \"sales\"      : [ \n" + "                     { \"firstName\" : \"Sally\", \n"
        + "                       \"lastName\"  : \"Green\",\n" + "                        \"age\"      : 27 },\n"
        + "\n" + "                     { \"firstName\" : \"Jim\",   \n"
        + "                       \"lastName\"  : \"Galley\",\n" + "                       \"age\"       : 41 }\n"
        + "                 ] \n" + "} ";

    String json = json3;
    System.out.println("json = " + json);
    JsonNode rawJsonNode = new ObjectMapper().readTree(json);

    System.out.println(
        "rawJsonNode = " + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(rawJsonNode));
    String flattenJson = JsonFlattener.flatten(json);

    System.out.println("flattenJson = " + flattenJson);
    JsonNode jsonNode = new ObjectMapper().readTree(flattenJson);

    System.out
        .println("jsonNode = " + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));
    Map<String, Object> stringObjectMap = JsonFlattener.flattenAsMap(json);
    System.out.println("JsonFlattener.flattenAsMap(json) = " + stringObjectMap);
    NestedObjectIndexCreator creator = new NestedObjectIndexCreator(null, null, null);
    List<Map<String, String>> maps = creator.unnestJson(rawJsonNode);
    System.out.println("maps = " + maps.toString().replaceAll("},", "}\n"));
//    creator.add(json.getBytes());
  }
}

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
import com.google.common.io.Files;
import com.google.common.primitives.UnsignedBytes;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.io.util.VarLengthBytesValueReaderWriter;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.JSONMatchFilterOperator;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.predicate.EqPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.segment.index.readers.JSONIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class JSONIndexCreator implements Closeable {

  //separator used to join the key and value to create posting list key
  public static String POSTING_LIST_KEY_SEPARATOR = "|";
  static int FLUSH_THRESHOLD = 50_000;
  static int VERSION = 1;
  private final File flattenedDocId2RootDocIdMappingFile;
  private final File postingListFile;
  private File dictionaryheaderFile;
  private File dictionaryOffsetFile;
  private File dictionaryFile;
  private File invertedIndexOffsetFile;
  private File invertedIndexFile;
  private File outputIndexFile;

  private int docId = 0;
  private int numFlatennedDocId = 0;
  int chunkId = 0;

  private DataOutputStream postingListWriter;
  private DataOutputStream flattenedDocId2RootDocIdWriter;

  Map<String, List<Integer>> postingListMap = new TreeMap<>();
  List<Integer> flattenedDocIdList = new ArrayList<>();
  List<Integer> postingListChunkOffsets = new ArrayList<>();
  List<Integer> chunkLengths = new ArrayList<>();
  private FieldSpec fieldSpec;

  public JSONIndexCreator(File indexDir, FieldSpec fieldSpec, FieldSpec.DataType valueType)
      throws IOException {
    this.fieldSpec = fieldSpec;
    System.out.println("indexDir = " + indexDir);

    String name = fieldSpec.getName();
    postingListFile = new File(indexDir + name + "_postingList.buf");
    postingListWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(postingListFile)));
    postingListChunkOffsets.add(postingListWriter.size());

    dictionaryheaderFile = new File(indexDir, name + "_dictionaryHeader.buf");
    dictionaryOffsetFile = new File(indexDir, name + "_dictionaryOffset.buf");
    dictionaryFile = new File(indexDir, name + "_dictionary.buf");
    invertedIndexOffsetFile = new File(indexDir, name + "_invertedIndexOffset.buf");
    invertedIndexFile = new File(indexDir, name + "_invertedIndex.buf");
    flattenedDocId2RootDocIdMappingFile = new File(indexDir, name + "_flattenedDocId.buf");
    flattenedDocId2RootDocIdWriter =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(flattenedDocId2RootDocIdMappingFile)));

    //output file
    outputIndexFile = new File(indexDir, name + ".nested.idx");
  }

  public void add(byte[] data)
      throws IOException {

    JsonNode jsonNode = new ObjectMapper().readTree(data);
    List<Map<String, String>> flattenedMapList = unnestJson(jsonNode);
    for (Map<String, String> map : flattenedMapList) {
      //
      for (Map.Entry<String, String> entry : map.entrySet()) {
        //handle key posting list
        String key = entry.getKey();

        List<Integer> keyPostingList = postingListMap.get(key);
        if (keyPostingList == null) {
          keyPostingList = new ArrayList<>();
          postingListMap.put(key, keyPostingList);
        }
        keyPostingList.add(numFlatennedDocId);

        //handle keyvalue posting list
        String keyValue = key + POSTING_LIST_KEY_SEPARATOR + entry.getValue();
        List<Integer> keyValuePostingList = postingListMap.get(keyValue);
        if (keyValuePostingList == null) {
          keyValuePostingList = new ArrayList<>();
          postingListMap.put(keyValue, keyValuePostingList);
        }
        keyValuePostingList.add(numFlatennedDocId);
      }
      //flattenedDocId2RootDocIdMapping
      flattenedDocIdList.add(numFlatennedDocId);

      numFlatennedDocId++;
    }
    docId++;

    //flush data
    if (docId % FLUSH_THRESHOLD == 0) {
      flush();
    }
  }

  /**
   * Multi value
   * @param dataArray
   * @param length
   * @throws IOException
   */
  public void add(byte[][] dataArray, int length)
      throws IOException {

    for (int i = 0; i < length; i++) {
      byte[] data = dataArray[i];
      JsonNode jsonNode = new ObjectMapper().readTree(data);
      List<Map<String, String>> flattenedMapList = unnestJson(jsonNode);
      for (Map<String, String> map : flattenedMapList) {
        //
        for (Map.Entry<String, String> entry : map.entrySet()) {
          //handle key posting list
          String key = entry.getKey();

          List<Integer> keyPostingList = postingListMap.get(key);
          if (keyPostingList == null) {
            keyPostingList = new ArrayList<>();
            postingListMap.put(key, keyPostingList);
          }
          keyPostingList.add(numFlatennedDocId);

          //handle keyvalue posting list
          String keyValue = key + POSTING_LIST_KEY_SEPARATOR + entry.getValue();
          List<Integer> keyValuePostingList = postingListMap.get(keyValue);
          if (keyValuePostingList == null) {
            keyValuePostingList = new ArrayList<>();
            postingListMap.put(keyValue, keyValuePostingList);
          }
          keyValuePostingList.add(numFlatennedDocId);
        }
        //flattenedDocId2RootDocIdMapping
        flattenedDocIdList.add(numFlatennedDocId);

        numFlatennedDocId++;
      }
    }
    docId++;

    //flush data
    if (docId % FLUSH_THRESHOLD == 0) {
      flush();
    }
  }

  public void seal()
      throws IOException {

    flush();

    flattenedDocId2RootDocIdWriter.close();
    postingListWriter.close();

    //key posting list merging
    System.out.println("InvertedIndex");
    System.out.println("=================");

    int maxKeyLength = createInvertedIndex(postingListFile, postingListChunkOffsets, chunkLengths);
    System.out.println("=================");

    int flattenedDocid = 0;
    DataInputStream flattenedDocId2RootDocIdReader =
        new DataInputStream(new BufferedInputStream(new FileInputStream(flattenedDocId2RootDocIdMappingFile)));
    int[] rootDocIdArray = new int[numFlatennedDocId];
    while (flattenedDocid < numFlatennedDocId) {
      rootDocIdArray[flattenedDocid++] = flattenedDocId2RootDocIdReader.readInt();
    }
    System.out.println("FlattenedDocId  to RootDocId Mapping = ");
    System.out.println(Arrays.toString(rootDocIdArray));

    //PUT all contents into one file

    //header
    // version + maxDictionaryLength + [store the offsets + length for each one (dictionary offset file, dictionaryFile, index offset file, index file, flattened docId to rootDocId file)]
    long headerSize = 2 * Integer.BYTES + 6 * 2 * Long.BYTES;

    long dataSize =
        dictionaryheaderFile.length() + dictionaryOffsetFile.length() + dictionaryFile.length() + invertedIndexFile
            .length() + invertedIndexOffsetFile.length() + flattenedDocId2RootDocIdMappingFile.length();

    long totalSize = headerSize + dataSize;
    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.mapFile(outputIndexFile, false, 0, totalSize, ByteOrder.BIG_ENDIAN, "Nested inverted index");

    pinotDataBuffer.putInt(0, VERSION);
    pinotDataBuffer.putInt(1 * Integer.BYTES, maxKeyLength);
    long writtenBytes = headerSize;

    //add dictionary header
    int bufferId = 0;
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId), writtenBytes);
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId) + Long.BYTES, dictionaryheaderFile.length());
    pinotDataBuffer.readFrom(writtenBytes, dictionaryheaderFile, 0, dictionaryheaderFile.length());
    writtenBytes += dictionaryheaderFile.length();

    //add dictionary offset
    bufferId = bufferId + 1;
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId), writtenBytes);
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId) + Long.BYTES, dictionaryOffsetFile.length());
    pinotDataBuffer.readFrom(writtenBytes, dictionaryOffsetFile, 0, dictionaryOffsetFile.length());
    writtenBytes += dictionaryOffsetFile.length();

    //add dictionary
    bufferId = bufferId + 1;
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId), writtenBytes);
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId) + Long.BYTES, dictionaryFile.length());
    pinotDataBuffer.readFrom(writtenBytes, dictionaryFile, 0, dictionaryFile.length());
    writtenBytes += dictionaryFile.length();

    //add index offset
    bufferId = bufferId + 1;
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId), writtenBytes);
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId) + Long.BYTES, invertedIndexOffsetFile.length());
    pinotDataBuffer.readFrom(writtenBytes, invertedIndexOffsetFile, 0, invertedIndexOffsetFile.length());
    writtenBytes += invertedIndexOffsetFile.length();

    //add index data
    bufferId = bufferId + 1;
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId), writtenBytes);
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId) + Long.BYTES, invertedIndexFile.length());
    pinotDataBuffer.readFrom(writtenBytes, invertedIndexFile, 0, invertedIndexFile.length());
    writtenBytes += invertedIndexFile.length();

    //add flattened docid to root doc id mapping
    bufferId = bufferId + 1;
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId), writtenBytes);
    pinotDataBuffer.putLong(getBufferStartOffset(bufferId) + Long.BYTES, flattenedDocId2RootDocIdMappingFile.length());
    pinotDataBuffer
        .readFrom(writtenBytes, flattenedDocId2RootDocIdMappingFile, 0, flattenedDocId2RootDocIdMappingFile.length());
    writtenBytes += flattenedDocId2RootDocIdMappingFile.length();
  }

  private long getBufferStartOffset(int bufferId) {
    return 2 * Integer.BYTES + 2 * bufferId * Long.BYTES;
  }

  private int createInvertedIndex(File postingListFile, List<Integer> postingListChunkOffsets,
      List<Integer> chunkLengthList)
      throws IOException {

    List<Iterator<ImmutablePair<byte[], int[]>>> chunkIterators = new ArrayList<>();

    for (int i = 0; i < chunkLengthList.size(); i++) {

      final DataInputStream postingListFileReader =
          new DataInputStream(new BufferedInputStream(new FileInputStream(postingListFile)));
      postingListFileReader.skipBytes(postingListChunkOffsets.get(i));
      final int length = chunkLengthList.get(i);
      chunkIterators.add(new Iterator<ImmutablePair<byte[], int[]>>() {
        int index = 0;

        @Override
        public boolean hasNext() {
          return index < length;
        }

        @Override
        public ImmutablePair<byte[], int[]> next() {
          try {
            int keyLength = postingListFileReader.readInt();
            byte[] keyBytes = new byte[keyLength];
            postingListFileReader.read(keyBytes);

            int postingListLength = postingListFileReader.readInt();
            int[] postingList = new int[postingListLength];
            for (int i = 0; i < postingListLength; i++) {
              postingList[i] = postingListFileReader.readInt();
            }
            index++;
            return ImmutablePair.of(keyBytes, postingList);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    final Comparator<byte[]> byteArrayComparator = UnsignedBytes.lexicographicalComparator();

    PriorityQueue<ImmutablePair<Integer, ImmutablePair<byte[], int[]>>> queue =
        new PriorityQueue<>(chunkLengthList.size(),
            (o1, o2) -> byteArrayComparator.compare(o1.getRight().getLeft(), o2.getRight().getLeft()));
    for (int i = 0; i < chunkIterators.size(); i++) {
      Iterator<ImmutablePair<byte[], int[]>> iterator = chunkIterators.get(i);
      if (iterator.hasNext()) {
        queue.offer(ImmutablePair.of(i, iterator.next()));
      }
    }
    byte[] prevKey = null;
    RoaringBitmap roaringBitmap = new RoaringBitmap();

    Writer writer = new Writer(dictionaryheaderFile, dictionaryOffsetFile, dictionaryFile, invertedIndexOffsetFile,
        invertedIndexFile);
    while (!queue.isEmpty()) {
      ImmutablePair<Integer, ImmutablePair<byte[], int[]>> poll = queue.poll();
      byte[] currKey = poll.getRight().getLeft();
      if (prevKey != null && byteArrayComparator.compare(prevKey, currKey) != 0) {
        System.out.println(new String(prevKey) + ":" + roaringBitmap);
        writer.add(prevKey, roaringBitmap);
        roaringBitmap.clear();
      }

      roaringBitmap.add(poll.getRight().getRight());
      prevKey = currKey;

      //add the next key from the chunk where the currKey was removed from
      Iterator<ImmutablePair<byte[], int[]>> iterator = chunkIterators.get(poll.getLeft());
      if (iterator.hasNext()) {
        queue.offer(ImmutablePair.of(poll.getLeft(), iterator.next()));
      }
    }

    if (prevKey != null) {
      writer.add(prevKey, roaringBitmap);
    }
    writer.finish();
    return writer.getMaxDictionaryValueLength();
  }

  private void flush()
      throws IOException {
    //write the key (length|actual bytes) - posting list(length, flattenedDocIds)
    System.out.println("postingListMap = " + postingListMap);
    for (Map.Entry<String, List<Integer>> entry : postingListMap.entrySet()) {
      byte[] keyBytes = entry.getKey().getBytes(Charset.forName("UTF-8"));
      postingListWriter.writeInt(keyBytes.length);
      postingListWriter.write(keyBytes);
      List<Integer> flattenedDocIdList = entry.getValue();
      postingListWriter.writeInt(flattenedDocIdList.size());
      for (int flattenedDocId : flattenedDocIdList) {
        postingListWriter.writeInt(flattenedDocId);
      }
    }

    //write flattened doc id to root docId mapping
    for (int rootDocId : flattenedDocIdList) {
      flattenedDocId2RootDocIdWriter.writeInt(rootDocId);
    }
    chunkLengths.add(postingListMap.size());
    postingListChunkOffsets.add(postingListWriter.size());
    postingListMap.clear();
    flattenedDocIdList.clear();
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

  @Override
  public void close()
      throws IOException {

  }

  private class Writer {
    private DataOutputStream _dictionaryHeaderWriter;
    private DataOutputStream _dictionaryOffsetWriter;
    private File _dictionaryOffsetFile;
    private DataOutputStream _dictionaryWriter;
    private DataOutputStream _invertedIndexOffsetWriter;
    private File _invertedIndexOffsetFile;
    private DataOutputStream _invertedIndexWriter;
    private int _dictId;
    private int _dictOffset;
    private int _invertedIndexOffset;
    int _maxDictionaryValueLength = Integer.MIN_VALUE;

    public Writer(File dictionaryheaderFile, File dictionaryOffsetFile, File dictionaryFile,
        File invertedIndexOffsetFile, File invertedIndexFile)
        throws IOException {
      _dictionaryHeaderWriter =
          new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dictionaryheaderFile)));

      _dictionaryOffsetWriter =
          new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dictionaryOffsetFile)));
      _dictionaryOffsetFile = dictionaryOffsetFile;
      _dictionaryWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dictionaryFile)));
      _invertedIndexOffsetWriter =
          new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexOffsetFile)));
      _invertedIndexOffsetFile = invertedIndexOffsetFile;
      _invertedIndexWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexFile)));
      _dictId = 0;
      _dictOffset = 0;
      _invertedIndexOffset = 0;
    }

    public void add(byte[] key, RoaringBitmap roaringBitmap)
        throws IOException {
      if (key.length > _maxDictionaryValueLength) {
        _maxDictionaryValueLength = key.length;
      }
      //write the key to dictionary
      _dictionaryOffsetWriter.writeInt(_dictOffset);
      _dictionaryWriter.write(key);

      //write the roaringBitmap to inverted index
      _invertedIndexOffsetWriter.writeInt(_invertedIndexOffset);

      int serializedSizeInBytes = roaringBitmap.serializedSizeInBytes();
      byte[] serializedRoaringBitmap = new byte[serializedSizeInBytes];
      ByteBuffer serializedRoaringBitmapBuffer = ByteBuffer.wrap(serializedRoaringBitmap);
      roaringBitmap.serialize(serializedRoaringBitmapBuffer);
      _invertedIndexWriter.write(serializedRoaringBitmap);
      System.out.println(
          "dictId = " + _dictId + ", dict offset:" + _dictOffset + ", valueLength:" + key.length + ", inv offset:"
              + _invertedIndexOffset + ", serializedSizeInBytes:" + serializedSizeInBytes);

      //increment offsets
      _dictOffset = _dictOffset + key.length;
      _invertedIndexOffset = _invertedIndexOffset + serializedSizeInBytes;
      //increment the dictionary id
      _dictId = _dictId + 1;
    }

    void finish()
        throws IOException {
      //InvertedIndexReader and VarlengthBytesValueReaderWriter needs one extra entry for offsets since it computes the length for index i using offset[i+1] - offset[i]
      _invertedIndexOffsetWriter.writeInt(_invertedIndexOffset);
      _dictionaryOffsetWriter.writeInt(_dictOffset);

      byte[] headerBytes = VarLengthBytesValueReaderWriter.getHeaderBytes(_dictId);
      _dictionaryHeaderWriter.write(headerBytes);
      System.out.println("headerBytes = " + Arrays.toString(headerBytes));

      _dictionaryHeaderWriter.close();
      _dictionaryOffsetWriter.close();
      _dictionaryWriter.close();
      _invertedIndexOffsetWriter.close();
      _invertedIndexWriter.close();

      //data offsets started with zero but the actual dictionary and index will contain (header + offsets + data). so all the offsets must be adjusted ( i.e add size(header) + size(offset) to each offset value)
      PinotDataBuffer dictionaryOffsetBuffer = PinotDataBuffer
          .mapFile(dictionaryOffsetFile, false, 0, _dictionaryOffsetFile.length(), ByteOrder.BIG_ENDIAN,
              "dictionary offset file");
      int dictOffsetBase = _dictionaryHeaderWriter.size() + _dictionaryOffsetWriter.size();
      for (int i = 0; i < _dictId + 1; i++) {
        int offset = dictionaryOffsetBuffer.getInt(i * Integer.BYTES);
        int newOffset = offset + dictOffsetBase;
        dictionaryOffsetBuffer.putInt(i * Integer.BYTES, offset + dictOffsetBase);
        System.out.println("dictId = " + i + ", offset = " + offset + ", newOffset = " + newOffset);
      }

      PinotDataBuffer invIndexOffsetBuffer = PinotDataBuffer
          .mapFile(invertedIndexOffsetFile, false, 0, invertedIndexOffsetFile.length(), ByteOrder.BIG_ENDIAN,
              "invertedIndexOffsetFile");
      int invIndexOffsetBase = _invertedIndexOffsetWriter.size();
      for (int i = 0; i < _dictId + 1; i++) {
        int offset = invIndexOffsetBuffer.getInt(i * Integer.BYTES);
        int newOffset = offset + invIndexOffsetBase;
        System.out.println("offset = " + offset + ", newOffset = " + newOffset);

        invIndexOffsetBuffer.putInt(i * Integer.BYTES, newOffset);
      }

      invIndexOffsetBuffer.close();
      dictionaryOffsetBuffer.close();
    }

    public int getMaxDictionaryValueLength() {
      return _maxDictionaryValueLength;
    }
  }

  public static void main(String[] args)
      throws Exception {

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
    FieldSpec fieldSpec = new DimensionFieldSpec();
    fieldSpec.setName("person");
    File tempDir = Files.createTempDir();
    JSONIndexCreator creator = new JSONIndexCreator(tempDir, fieldSpec, FieldSpec.DataType.BYTES);
    List<Map<String, String>> maps = creator.unnestJson(rawJsonNode);
    System.out.println("maps = " + maps.toString().replaceAll("},", "}\n"));
    creator.add(json.getBytes());

    creator.seal();
    System.out.println("Output Dir = " + tempDir);
    System.out.println("FileUtils.listFiles(tempDir, null, true) = " + FileUtils.listFiles(tempDir, null, true).stream()
        .map(file -> file.getName()).collect(Collectors.toList()));

    //Test reader
    PinotDataBuffer buffer =
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(tempDir, fieldSpec.getName() + ".nested.idx"));
    JSONIndexReader reader = new JSONIndexReader(buffer);
    ExpressionContext lhs = ExpressionContext.forIdentifier("person");
    Predicate predicate = new EqPredicate(lhs, "addresses.street" + POSTING_LIST_KEY_SEPARATOR + "third st");
    MutableRoaringBitmap matchingDocIds = reader.getMatchingDocIds(predicate);
    System.out.println("matchingDocIds = " + matchingDocIds);

    //Test filter operator
    FilterContext filterContext = QueryContextConverterUtils
        .getFilter(CalciteSqlParser.compileToExpression("name='adam' AND addresses.street='main st'"));
    int numDocs = 1;
    JSONMatchFilterOperator operator = new JSONMatchFilterOperator("person", filterContext, reader, numDocs);
    FilterBlock filterBlock = operator.nextBlock();
    BlockDocIdIterator iterator = filterBlock.getBlockDocIdSet().iterator();
    int docId = -1;
    while ((docId = iterator.next()) > 0) {
      System.out.println("docId = " + docId);
    }
  }
}
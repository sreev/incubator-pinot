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
package org.apache.pinot.core.segment.index.readers;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class JSONIndexReader implements Closeable {

  private static int EXPECTED_VERSION = 1;
  private static int DICT_HEADER_INDEX = 0;
  private static int DICT_OFFSET_INDEX = 1;
  private static int DICT_DATA_INDEX = 2;
  private static int INV_OFFSET_INDEX = 3;
  private static int INV_DATA_INDEX = 4;
  private static int FLATTENED_2_ROOT_INDEX = 5;

  private final BitmapInvertedIndexReader invertedIndexReader;
  private final StringDictionary dictionary;
  private final long cardinality;
  private final long numFlattenedDocs;

  public JSONIndexReader(PinotDataBuffer pinotDataBuffer) {

    int version = pinotDataBuffer.getInt(0);
    int maxKeyLength = pinotDataBuffer.getInt(1 * Integer.BYTES);

    Preconditions.checkState(version == EXPECTED_VERSION, String
        .format("Index version:{} is not supported by this reader. expected version:{}", version, EXPECTED_VERSION));

    // dictionaryHeaderFile, dictionaryOffsetFile, dictionaryFile, invIndexOffsetFile, invIndexFile, FlattenedDocId2DocIdMappingFile
    int numBuffers = 6;
    long bufferStartOffsets[] = new long[numBuffers];
    long bufferSizeArray[] = new long[numBuffers];
    for (int i = 0; i < numBuffers; i++) {
      bufferStartOffsets[i] = pinotDataBuffer.getLong(2 * Integer.BYTES + 2 * i * Long.BYTES);
      bufferSizeArray[i] = pinotDataBuffer.getLong(2 * Integer.BYTES + 2 * i * Long.BYTES + Long.BYTES);
    }
    cardinality = bufferSizeArray[DICT_OFFSET_INDEX] / Integer.BYTES - 1;
    numFlattenedDocs = bufferSizeArray[FLATTENED_2_ROOT_INDEX] / Integer.BYTES;

    long dictionaryStartOffset = bufferStartOffsets[DICT_HEADER_INDEX];
    long dictionarySize =
        bufferSizeArray[DICT_HEADER_INDEX] + bufferSizeArray[DICT_OFFSET_INDEX] + bufferSizeArray[DICT_DATA_INDEX];

    //TODO: REMOVE DEBUG START
    byte[] dictHeaderBytes = new byte[(int) bufferSizeArray[DICT_HEADER_INDEX]];
    pinotDataBuffer.copyTo(bufferStartOffsets[DICT_HEADER_INDEX], dictHeaderBytes);
    System.out.println("Arrays.toString(dictHeaderBytes) = " + Arrays.toString(dictHeaderBytes));
    //TODO: REMOVE DEBUG  END

    PinotDataBuffer dictionaryBuffer =
        pinotDataBuffer.view(dictionaryStartOffset, dictionaryStartOffset + dictionarySize);
    dictionary = new StringDictionary(dictionaryBuffer, (int) cardinality, maxKeyLength, Byte.valueOf("0"));

    long invIndexStartOffset = bufferStartOffsets[INV_OFFSET_INDEX];
    long invIndexSize = bufferSizeArray[INV_OFFSET_INDEX] + bufferSizeArray[INV_DATA_INDEX];

    PinotDataBuffer invIndexBuffer = pinotDataBuffer.view(invIndexStartOffset, invIndexStartOffset + invIndexSize);
    invertedIndexReader = new BitmapInvertedIndexReader(invIndexBuffer, (int) cardinality);

    //TODO: REMOVE DEBUG START
    for (int dictId = 0; dictId < dictionary.length(); dictId++) {
      System.out.println("Key = " + new String(dictionary.getBytes(dictId)));
      System.out.println("Posting List = " + invertedIndexReader.getDocIds(dictId));
    }
    //TODO: REMOVE DEBUG  END

  }

  /**
   * Returns the matching document ids for the given search query.
   */
  public MutableRoaringBitmap getMatchingDocIds(Predicate predicate) {

    PredicateEvaluator predicateEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dictionary, FieldSpec.DataType.BYTES);
    boolean exclusive = predicateEvaluator.isExclusive();
    int[] dictIds = exclusive ? predicateEvaluator.getNonMatchingDictIds() : predicateEvaluator.getMatchingDictIds();
    int numDictIds = dictIds.length;

    if (numDictIds == 1) {
      ImmutableRoaringBitmap docIds = (ImmutableRoaringBitmap) invertedIndexReader.getDocIds(dictIds[0]);
      if (exclusive) {
        if (docIds instanceof MutableRoaringBitmap) {
          MutableRoaringBitmap mutableRoaringBitmap = (MutableRoaringBitmap) docIds;
          mutableRoaringBitmap.flip(0L, numFlattenedDocs);
          return mutableRoaringBitmap;
        } else {
          return ImmutableRoaringBitmap.flip(docIds, 0L, numFlattenedDocs);
        }
      } else {
        return docIds.toMutableRoaringBitmap();
      }
    } else {
      ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numDictIds];
      for (int i = 0; i < numDictIds; i++) {
        bitmaps[i] = (ImmutableRoaringBitmap) invertedIndexReader.getDocIds(dictIds[i]);
      }
      MutableRoaringBitmap docIds = ImmutableRoaringBitmap.or(bitmaps);
      if (exclusive) {
        docIds.flip(0L, numFlattenedDocs);
      }
      return docIds;
    }
  }

  @Override
  public void close()
      throws IOException {

  }
}

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
package org.apache.pinot.core.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.impl.inv.JSONIndexCreator;
import org.apache.pinot.core.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitMVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitSVForwardIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION;


@SuppressWarnings({"rawtypes", "unchecked"})
public class JSONIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(JSONIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _jsonIndexColumns = new HashSet<>();

  public JSONIndexHandler(File indexDir, SegmentMetadataImpl segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    // Only create json index on dictionary-encoded unsorted columns
    for (String column : indexLoadingConfig.getJsonIndexColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        _jsonIndexColumns.add(columnMetadata);
      }
    }
  }

  public void createJsonIndices()
      throws IOException {
    for (ColumnMetadata columnMetadata : _jsonIndexColumns) {
      createJSONIndexForColumn(columnMetadata);
    }
  }

  private void createJSONIndexForColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String column = columnMetadata.getColumnName();

    File inProgress = new File(_indexDir, column + JSON_INDEX_FILE_EXTENSION + ".inprogress");
    File jsonIndexFile = new File(_indexDir, column + JSON_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.JSON_INDEX)) {
        // Skip creating json index if already exists.

        LOGGER.info("Found json index for segment: {}, column: {}", _segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove json index if exists.
      // For v1 and v2, it's the actual json index. For v3, it's the temporary json index.
      FileUtils.deleteQuietly(jsonIndexFile);
    }

    // Create new json index for the column.
    LOGGER.info("Creating new json index for segment: {}, column: {}", _segmentName, column);
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(columnMetadata);
    }

    // For v3, write the generated json index file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, jsonIndexFile, ColumnIndexType.JSON_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created json index for segment: {}, column: {}", _segmentName, column);
  }

  private void handleDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws IOException {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata, _segmentWriter);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = getDictionaryReader(columnMetadata, _segmentWriter);
        JSONIndexCreator jsonIndexCreator = new JSONIndexCreator(_indexDir, columnMetadata.getFieldSpec(),
            FieldSpec.DataType.BYTES)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column
        for (int i = 0; i < numDocs; i++) {
          int dictId = forwardIndexReader.getDictId(i, readerContext);
          jsonIndexCreator.add(dictionary.getBytesValue(dictId));
        }
      } else {
        // Multi-value column
        throw new IllegalStateException("JSON Indexing is not supported on multi-valued columns ");
      }
      jsonIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws IOException {
    FieldSpec.DataType dataType = columnMetadata.getDataType();
    if(dataType != FieldSpec.DataType.BYTES || dataType != FieldSpec.DataType.STRING) {
      throw new UnsupportedOperationException("JSON indexing is only supported for STRING/BYTES datatype but found: "+ dataType);
    }
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata, _segmentWriter);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        JSONIndexCreator jsonIndexCreator = new JSONIndexCreator(_indexDir, columnMetadata.getFieldSpec(),
            dataType)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.
        switch (dataType) {
          case STRING:
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              jsonIndexCreator.add(forwardIndexReader.getBytes(i, readerContext));
            }
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + dataType);
        }
      } else {
        // Multi-value column
        switch (dataType) {
          default:
            throw new IllegalStateException("JSON Indexing is not supported on multi-valued columns ");
        }
      }
      jsonIndexCreator.seal();
    }
  }

  private ForwardIndexReader<?> getForwardIndexReader(ColumnMetadata columnMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    int numRows = columnMetadata.getTotalDocs();
    int numBitsPerValue = columnMetadata.getBitsPerElement();
    if (columnMetadata.isSingleValue()) {
      return new FixedBitSVForwardIndexReader(buffer, numRows, numBitsPerValue);
    } else {
      return new FixedBitMVForwardIndexReader(buffer, numRows, columnMetadata.getTotalNumberOfEntries(),
          numBitsPerValue);
    }
  }

  private BaseImmutableDictionary getDictionaryReader(ColumnMetadata columnMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    BaseImmutableDictionary dictionary = PhysicalColumnIndexContainer.loadDictionary(buffer, columnMetadata, false);
    return dictionary;
  }
}

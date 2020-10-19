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
package org.apache.pinot.core.operator.filter;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.predicate.EqPredicate;
import org.apache.pinot.core.query.request.context.predicate.InPredicate;
import org.apache.pinot.core.query.request.context.predicate.NotEqPredicate;
import org.apache.pinot.core.query.request.context.predicate.NotInPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.segment.creator.impl.inv.JSONIndexCreator;
import org.apache.pinot.core.segment.index.readers.JSONIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


@SuppressWarnings("rawtypes")
public class JSONMatchFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "JSONMatchFilterOperator";

  private final JSONIndexReader _nestedObjectIndexReader;
  private final int _numDocs;
  private String _column;
  private final FilterContext _filterContext;

  public JSONMatchFilterOperator(String column, FilterContext filterContext, JSONIndexReader nestedObjectIndexReader,
      int numDocs) {
    _column = column;
    _filterContext = filterContext;
    _nestedObjectIndexReader = nestedObjectIndexReader;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    ImmutableRoaringBitmap docIds = process(_filterContext);
    System.out.println("docIds = " + docIds);
    return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs));
  }

  private MutableRoaringBitmap process(FilterContext filterContext) {
    List<FilterContext> children = _filterContext.getChildren();
    MutableRoaringBitmap resultBitmap = null;

    switch (filterContext.getType()) {
      case AND:
        for (FilterContext child : children) {
          if (resultBitmap == null) {
            resultBitmap = process(child);
          } else {
            resultBitmap.and(process(child));
          }
        }
        break;
      case OR:
        for (FilterContext child : children) {
          if (resultBitmap == null) {
            resultBitmap = process(child);
          } else {
            resultBitmap.or(process(child));
          }
        }
        break;
      case PREDICATE:
        Predicate predicate = filterContext.getPredicate();
        Predicate newPredicate = null;
        switch (predicate.getType()) {

          case EQ:
            EqPredicate eqPredicate = (EqPredicate) predicate;
            newPredicate = new EqPredicate(ExpressionContext.forIdentifier(_column),
                eqPredicate.getLhs().getIdentifier() + JSONIndexCreator.POSTING_LIST_KEY_SEPARATOR + eqPredicate
                    .getValue());
            break;
          case NOT_EQ:
            NotEqPredicate nEqPredicate = (NotEqPredicate) predicate;
            newPredicate = new NotEqPredicate(ExpressionContext.forIdentifier(_column),
                nEqPredicate.getLhs().getIdentifier() + JSONIndexCreator.POSTING_LIST_KEY_SEPARATOR
                    + nEqPredicate.getValue());
            break;
          case IN:
            InPredicate inPredicate = (InPredicate) predicate;
            List<String> newInValues = inPredicate.getValues().stream().map(
                value -> inPredicate.getLhs().getIdentifier() + JSONIndexCreator.POSTING_LIST_KEY_SEPARATOR
                    + value).collect(Collectors.toList());
            newPredicate = new InPredicate(ExpressionContext.forIdentifier(_column), newInValues);
            break;
          case NOT_IN:
            NotInPredicate notInPredicate = (NotInPredicate) predicate;
            List<String> newNotInValues = inPredicate.getValues().stream().map(
                value -> notInPredicate.getLhs().getIdentifier() + JSONIndexCreator.POSTING_LIST_KEY_SEPARATOR
                    + value).collect(Collectors.toList());
            newPredicate = new InPredicate(ExpressionContext.forIdentifier(_column), newNotInValues);
            break;
          case IS_NULL:
            newPredicate = predicate;
            break;
          case IS_NOT_NULL:
            newPredicate = predicate;
            break;
          case RANGE:
          case REGEXP_LIKE:
          case TEXT_MATCH:
            throw new UnsupportedOperationException("JSON Match does not support RANGE, REGEXP or TEXTMATCH");
        }

        resultBitmap = _nestedObjectIndexReader.getMatchingDocIds(newPredicate);
        break;
    }
    return resultBitmap;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}

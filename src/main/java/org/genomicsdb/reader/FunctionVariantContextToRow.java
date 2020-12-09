/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Omics Data Automation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.genomicsdb.reader;

import scala.Function1;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import htsjdk.variant.variantcontext.VariantContext;
import java.util.stream.Collectors;
import java.io.Serializable;

public class FunctionVariantContextToRow extends scala.runtime.AbstractFunction1<scala.Tuple2<String,VariantContext>,Row> implements Serializable {

  public FunctionVariantContextToRow(){}

  @Override
  public Row apply(scala.Tuple2<String,VariantContext> x){
    return RowFactory.create(
      x._2().getContig(),
      x._2().getStart(),
      x._2().getID(),
      x._2().getType().toString(),
      x._2().getReference().toString(),
      x._2().getAlternateAlleles().stream().map(a -> a.getDisplayString()).collect(Collectors.toList()),
      x._2().getSampleNames(),
      x._2().getGenotypes().stream().map(g -> g.toString()).collect(Collectors.toList()));
  }

}

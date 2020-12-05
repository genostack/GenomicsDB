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

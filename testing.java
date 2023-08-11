        Dataset<Row> joinedDfAfterReturnExcluded = dfSrc.join(functions.broadcast(dfReturns), dfSrc.col("Order ID").equalTo(dfReturns.col("Order ID")), "leftsemi");

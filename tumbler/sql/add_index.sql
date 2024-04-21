
LOCK TABLES `kline_1min` WRITE;
ALTER  TABLE  `kline_1min`  ADD  INDEX symbol_1min (  `symbol`  );
ALTER  TABLE  `kline_1min`  ADD  INDEX datetime_1min ( `datetime` );
LOCK TABLES `kline_1min` WRITE;


LOCK TABLES `kline_1hour` WRITE;
ALTER  TABLE  `kline_1hour`  ADD  INDEX symbol_1hour (  `symbol`  );
ALTER  TABLE  `kline_1hour`  ADD  INDEX datetime_1hour ( `datetime` );
LOCK TABLES `kline_1hour` WRITE;



//maxwell
//retain
{"database":"gmall","table":"base_trademark","type":"insert","ts":1683276840,"xid":167,"commit":true,"data":{"id":13,"tm_name":"ljw","logo_url":"/aaa/aaa"}}
{"database":"gmall","table":"base_trademark","type":"update","ts":1683276840,"xid":188,"commit":true,"data":{"id":13,"tm_name":"ljw","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
{"database":"gmall","table":"base_trademark","type":"bootstrap-insert","ts":1683276840,"data":{"id":1,"tm_name":"三星","logo_url":"/static/default.jpg"}}

//filter
{"database":"gmall","table":"base_trademark","type":"delete","ts":1683276840,"xid":201,"commit":true,"data":{"id":13,"tm_name":"ljw","logo_url":"/bbb/bbb"}}
{"database":"gmall","table":"base_trademark","type":"bootstrap-start","ts":1683276840,"data":{}}
{"database":"gmall","table":"base_trademark","type":"bootstrap-complete","ts":1683276840,"data":{}}



//flink cdc
{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652513039549,"snapshot":"false","db":"gmall-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1683276840551,"transaction":null}


//start
{"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone Xs Max","mid":"mid_51315","os":"iOS 13.2.3","uid":"603","vc":"v2.1.132"},"start":{"entry":"notice","loading_time":1087,"open_ad_id":1,"open_ad_ms":9832,"open_ad_skip_ms":0},"ts":16832768400000}

{"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone Xs Max","mid":"mid_51315","os":"iOS 13.2.3","uid":"603","vc":"v2.1.132"},"start":{"entry":"notice","loading_time":1087,"open_ad_id":1,"open_ad_ms":9832,"open_ad_skip_ms":0},"ts":1683276840000}

{"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone Xs Max","mid":"mid_51315","os":"iOS 13.2.3","uid":"603","vc":"v2.1.132"},"start":{"entry":"notice","loading_time":1087,"open_ad_id":1,"open_ad_ms":9832,"open_ad_skip_ms":0},"ts":1683276840000}

//uv    
{"common":{"ar":"370000","ba":"Xiaomi","ch":"web","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_2190279","os":"Android 11.0","uid":"688","vc":"v2.1.134"},"page":{"during_time":11863,"item":"34,27,14","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},"ts":1683276840000}

{"common":{"ar":"370000","ba":"Xiaomi","ch":"web","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_2190279","os":"Android 11.0","uid":"688","vc":"v2.1.134"},"page":{"during_time":11863,"item":"34,27,14","item_type":"sku_ids","page_id":"trade"},"ts":1683276840000}

{"common":{"ar":"370000","ba":"Xiaomi","ch":"web","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_2190279","os":"Android 11.0","uid":"688","vc":"v2.1.134"},"page":{"during_time":11863,"item":"34,27,14","item_type":"sku_ids","page_id":"trade"},"ts":1683276840000}

{"common":{"ar":"370000","ba":"Xiaomi","ch":"web","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_2190280","os":"Android 11.0","uid":"688","vc":"v2.1.134"},"page":{"during_time":11863,"item":"34,27,14","item_type":"sku_ids","page_id":"trade"},"ts":1683276840000}

{"common":{"ar":"370000","ba":"Xiaomi","ch":"web","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_2190279","os":"Android 11.0","uid":"688","vc":"v2.1.134"},"page":{"during_time":11863,"item":"34,27,14","item_type":"sku_ids","page_id":"trade"},"ts":1683276840000}
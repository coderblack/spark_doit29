-- 建表语句
create table doit29.app_log(
  account         string,              
  appid           string,              
  appversion      string,              
  carrier         string,              
  deviceid        string,              
  devicetype      string,              
  eventid         string,              
  ip              string,              
  latitude        double,              
  longitude       double,              
  nettype         string,              
  osname          string,              
  osversion       string,              
  properties      map<string,string>,              
  releasechannel  string,              
  resolution      string,              
  sessionid       string,              
  `timestamp`       bigint              
)
partitioned by (dt string)
row format 
 serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
;

-- 导入数据语句
load data local inpath '/root/app.access.log.2021-12-10.gz' into table doit29.app_log partition(dt='2021-12-10');

drop if exists table dwt_db.dwt_test;
create table dwt_db.dwt_test(
  aa     string,
  bb     int,
  cc     string
)
   row format delimited
       fields terminated by '\t'
	   lines  terminated by '\n';
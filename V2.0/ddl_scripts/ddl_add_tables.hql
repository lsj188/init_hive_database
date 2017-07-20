drop table dim_db.dim_test1;
create table dim_db.dim_test1(
  aa     string,
  bb     int,
  cc     string
)
   row format delimited
       fields terminated by '\t'
	   lines  terminated by '\n';

drop table dim_db.dim_test2;
create table dim_db.dim_test2(
  aa     string,
  bb     int,
  cc     string
)
   row format delimited
       fields terminated by '\t'
	   lines  terminated by '\n';
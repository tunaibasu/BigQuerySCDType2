create or replace procedure `exalted-tape-316818.hr_data.sp_cdc`()

BEGIN 

create temp table edlvl_cdc_table
as 
select *,
       (CASE 
         WHEN mod_rec.EDLVL is null THEN 'Insert' 
         WHEN new_mod_rec.EDLVL_NEW is null THEN 'Delete' 
         WHEN mod_rec.EDLVL = new_mod_rec.EDLVL_NEW THEN 'Update' 
         else 'Undefined'
       END) as Operation_flag
from 

  (
  -----------------Records modified in Source ----------------------
  select EDLVLTYP,EDLVLTYPT,EDLVL,EDLVLT
  from
      (
      select EDLVLTYP,EDLVLTYPT,EDLVL,EDLVLT
      from `exalted-tape-316818.hr_data.dtedlvl_tgt`
      except distinct
      select EDLVLTYP,EDLVLTYPT,EDLVL,EDLVLT
      from `exalted-tape-316818.hr_data.dtedlvl_src`
      ) 
 ) as mod_rec
 
 FULL JOIN 
 (
  -----------------New/Modified Records in Source ----------------------
  select  EDLVLTYP as EDLVLTYP_NEW, EDLVLTYPT as EDLVLTYPT_NEW,EDLVL as EDLVL_NEW,EDLVLT as EDLVLT_NEW
  from 
    (
    select EDLVLTYP,EDLVLTYPT,EDLVL,EDLVLT
    from `exalted-tape-316818.hr_data.dtedlvl_src`
    except distinct
    select EDLVLTYP,EDLVLTYPT,EDLVL,EDLVLT
    from `exalted-tape-316818.hr_data.dtedlvl_tgt`
    ) 
  ) as new_mod_rec
  
  ON new_mod_rec.EDLVL_NEW = mod_rec.EDLVL;
 
#Insert records  
 insert into `exalted-tape-316818.hr_data.dtedlvl_tgt` 
 select EDLVLTYP_NEW as EDLVLTYP, EDLVLTYPT_NEW as EDLVLTYPT, EDLVL_NEW as EDLVL, 
 EDLVLT_NEW as EDLVLT, 
current_timestamp as start_dt,
current_timestamp as end_dt
 from edlvl_cdc_table where operation_flag='Insert';


#Update records
Update `exalted-tape-316818.hr_data.dtedlvl_tgt` tgt
set
    end_dt=current_timestamp
from edlvl_cdc_table tmp
where tmp.operation_flag = 'Update' and 
tgt.EDLVL = tmp.EDLVL;

insert `exalted-tape-316818.hr_data.dtedlvl_tgt` (EDLVLTYP,EDLVLTYPT,EDLVL,EDLVLT,start_dt,end_dt)
select EDLVLTYP_NEW,
    EDLVLTYPT_NEW, EDLVL_NEW,
    EDLVLT_NEW, 
    current_timestamp,
    NULL
from edlvl_cdc_table tmp
where tmp.operation_flag = 'Update';

#Update run_control_table
insert into `exalted-tape-316818.hr_data.run_control_table`
select 'dtedlvl_tgt' as tbl_nm, 
       'dtedlvl_src' as src_tbl_nm,
       sum(if(operation_flag='Insert',1,0)) as rec_inserted,
       sum(if(operation_flag='Delete',1,0)) as rec_deleted,
       sum(if(operation_flag='Update',1,0)) as rec_updated,
       current_timestamp as run_dt
       from edlvl_cdc_table;
        


drop  table edlvl_cdc_table;
END
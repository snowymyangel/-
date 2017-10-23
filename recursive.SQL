with 
recursive rectbl
(etl_system , etl_job , dependency_job , lvl )
as 
(
select etl_system , etl_job , dependency_job , 1(integer)
from petl.etl_job_dependency 
where etl_job  in ( 'ccr88_corp_cust_mger_size_info_mdm_1',
'ccr88_corp_cust_size_info_mdm_1',
'ccr88_c_biz_size_org_syn_info_mdm_1',
'ccr88_c_cust_cust_mger_sz_inf_mdm_1',
'ccr88_c_cust_ho_br_org_size_if_mdm_1',
'ccr88_mgmt_ckpt_org_index_sum3_mdm_1',
'ccr88_corp_cust_org_lv_syn_inf_mdm_1')
union all 
select   T.etl_system , T.etl_job , T.dependency_job , N.lvl+1
from 
rectbl N
inner join 
 petl.etl_job_dependency T
 on N.etl_job =  T.dependency_job 
  )
 sel distinct etl_job , dependency_job , lvl from rectbl
 where etl_job in ('ccr88_corp_cust_mger_size_info_mdm_1',
'ccr88_corp_cust_size_info_mdm_1',
'ccr88_c_biz_size_org_syn_info_mdm_1',
'ccr88_c_cust_cust_mger_sz_inf_mdm_1',
'ccr88_c_cust_ho_br_org_size_if_mdm_1',
'ccr88_mgmt_ckpt_org_index_sum3_mdm_1',
'ccr88_corp_cust_org_lv_syn_inf_mdm_1') and dependency_job in ('ccr88_corp_cust_mger_size_info_mdm_1',
'ccr88_corp_cust_size_info_mdm_1',
'ccr88_c_biz_size_org_syn_info_mdm_1',
'ccr88_c_cust_cust_mger_sz_inf_mdm_1',
'ccr88_c_cust_ho_br_org_size_if_mdm_1',
'ccr88_mgmt_ckpt_org_index_sum3_mdm_1',
'ccr88_corp_cust_org_lv_syn_inf_mdm_1')
 order by lvl;

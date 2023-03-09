-- Databricks notebook source
USE SX;
create or replace global temp view collaborators as 
select a.job_id, j.search_start_date, j.search_close_date, c.id as client_id, c.s5_comp_name, j.title, a.person_id, p.full_name_display, p.current_company, p.info:email_addresses as email_address, p.info:phone_numbers as phone_numbers, job_collaborator_type_id, b.name as collaborator_type
from job_collaborator a
inner join job_collaborator_type b on a.job_collaborator_type_id = b.id 
inner join person_v4 p on a.person_id = p.person_id
inner join job j on j.id = a.job_id
inner join client c on j.client_id = c.id
where j.is_test_job = false
order by a.created_at desc;

-- COMMAND ----------

select * from global_temp.collaborators;

-- COMMAND ----------

select *
from global_temp.collaborators
where job_collaborator_type_id = 13 and person_id = 21155
order by job_id;

-- COMMAND ----------

select * from sx.job_collaborator_type;

-- COMMAND ----------

-- Adhoc report for MM: list all collaborators on MM leading search (exclude rivi role)
select *
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 21155 
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "Kyle Langworthy" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 247712 
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "Gino Morell" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 100465 
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "Iain Grant" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 61996 
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "James Miller" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 36775 
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "Chris Rice" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 240048 
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select * from job_collaborator_type;

-- COMMAND ----------

select *, "John Twamley" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 249915
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "John Twamley" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 249915
  ) -- MM as lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "Dante Carpinito" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 165898 
  ) -- lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- COMMAND ----------

select *, "Dante Carpinito" as lead_recruiter
from global_temp.collaborators
where job_id in (
  select job_id 
  from global_temp.collaborators
  where job_collaborator_type_id = 13 and person_id = 165898 
  ) -- lead recruiter
and job_collaborator_type_id in (
  select id from sx.job_collaborator_type where is_rivi_role = false
  ) -- colloborators not rivi role
    ;

-- Databricks notebook source
-- current employment in public company with person

with targ_emp as (
-- current employment with public company
  select ec.id as employment_company_id, e.profile_id, employment_id, company_id, c.name as company_name, e.company_display as employment_company_display, title, start_date, end_date 
  from sx.employment_company ec
  inner join sx.company c on ec.company_id = c.id 
  inner join sx.employment e on ec.employment_id = e.id 
  where operating_status = 'public' and e.end_date is null),

  targ_emp_person as (
  -- current employment in public company with person
  select te.profile_id, company_name, employment_company_display, title, start_date, end_date, ps.id as person_id, full_name_display, p.location_display, linkedin_public_url
  from targ_emp te
  inner join sx.profile p on te.profile_id = p.id
  inner join sx.person ps on ps.profile_id = p.id),
  
  rivi_placed_job_person as (
  -- Display all rivi placed job and people
  select j.job_id, j.person_id, js.name as status, jb.title as job_title, search_start_date, search_close_date    
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join sx.job_candidate_stage js on j.job_candidate_stage_id = js.id
  inner join sx.person p on p.id = j.person_id
  where jb.is_retained = 1 and js.name = 'Placed')
  
select employment_company_display as current_employment, title as current_employment_title, start_date as current_employment_start_date, full_name_display as person, location_display as person_location, linkedin_public_url, search_close_date as rivi_placed_date
from targ_emp_person t1
inner join rivi_placed_job_person t2 on t1.person_id = t2.person_id;

-- COMMAND ----------

-- Display all rivi placed job and person_id
select j.job_id, j.person_id, js.name as status, jb.title as job_title, search_start_date, search_close_date    
from sx.job_candidate j
inner join sx.job jb on j.job_id = jb.id
inner join sx.job_candidate_stage js on j.job_candidate_stage_id = js.id
inner join sx.person p on p.id = j.person_id
where jb.is_retained = 1 and js.name = 'Placed';

-- COMMAND ----------

-- All Rivi placed candidates 
select distinct j.person_id, full_name_display
from sx.person p
inner join sx.job_candidate j on j.person_id = p.id
inner join sx.profile pf on p.profile_id = pf.id
where j.job_candidate_stage_id = 27 -- placed;

-- COMMAND ----------

-- Rivi placed employment
select e.* from sx.employment e
    inner join sx.employment_tag t on e.id = t.employment_id
    where tag_id = 32; -- 'Rivi placed'

-- COMMAND ----------

-- Person with rivi placed company employment
select p.id as person_id, e.profile_id, c.company_id, min(e.start_date) as start_date
    from employment e
    inner join employment_tag t on e.id = t.employment_id
    inner join employment_company c on e.id = c.employment_id
    inner join person p on p.profile_id = e.profile_id
    where tag_id = 32
    group by p.id, e.profile_id, c.company_id
    order by p.id, e.profile_id;    


-- COMMAND ----------

-- CEO request 1: Display all rivi placed jobs, candidates and placed date for clients which are public companies 
use sx;
with person_name as ( 
  select distinct person_id, full_name_display
  from person p
  inner join profile pf on p.profile_id = pf.id),

-- public client co
 public_co as (
   select c1.id as client_id, company_id, name as pub_co
    from client c1
    inner join company c2 on c1.company_id = c2.id
     where operating_status = 'public'),
     
-- Person with rivi placed company employment and employment start date
 place as ( 
    select p.id as person_id, e.profile_id, c.company_id, min(e.start_date) as placed_date
    from employment e
    inner join employment_tag t on e.id = t.employment_id
    inner join employment_company c on e.id = c.employment_id
    inner join person p on p.profile_id = e.profile_id
    where tag_id = 32
    group by p.id, e.profile_id, c.company_id
 )

select j.job_id, pub_co, 
    j.person_id, 
    full_name_display as placed_candidate, 
    js.name as status, 
    jb.title as job_title, 
    search_start_date, 
    search_close_date,
    placed_date
  from job_candidate j
  inner join job jb on j.job_id = jb.id
  inner join job_candidate_stage js on j.job_candidate_stage_id = js.id
  inner join person p on p.id = j.person_id
  inner join person_name n on p.id = n.person_id
  inner join public_co pc on pc.client_id = jb.client_id
  left join place pl on pl.person_id = j.person_id and pc.company_id = pl.company_id
  where jb.is_retained = 1 and js.name = 'Placed';

-- COMMAND ----------

-- CEO request2: all open pub company jobs and maximum stage candidates
  with ranks as (
    select job_id, person_id, job_candidate_stage_id, job_candidate_state_id,
        dense_rank() over(partition by job_id order by job_candidate_stage_id desc, job_candidate_state_id desc) as rnk -- find out max progress job canddiate 
    from sx.job_candidate 
  ), 
  
  person_name as (
  select distinct person_id, full_name_display
  from sx.person p
  inner join sx.profile pf on p.profile_id = pf.id),
  
  public_co as (
   select c1.id as client_id, company_id, name as pub_co
    from sx.client c1
    inner join sx.company c2 on c1.company_id = c2.id
     where operating_status = 'public')

  select j.job_id, j.person_id, pub_co, 
    full_name_display as candidate, 
    --j.job_candidate_stage_id, 
    s1.name as job_candidate_stage, 
    --j.job_candidate_state_id, 
    s2.name as job_candidate_state, 
    jb.title as job_title, search_start_date
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join ranks m on m.job_id = j.job_id and m.person_id = j.person_id
  inner join person_name n on n.person_id = j.person_id
  inner join public_co c on c.client_id = jb.client_id
  inner join sx.job_candidate_stage s1 on s1.id = j.job_candidate_stage_id
  inner join sx.job_candidate_state s2 on s2.id = j.job_candidate_state_id
  where jb.job_status_id = 5 --open job
    and rnk = 1 and j.job_candidate_state_id not in (45, 46, 47) -- recruiter, candidate, client reject  
  order by 1,2;

-- COMMAND ----------

-- Correct final version
-- CEO request2: all open pub company jobs and maximum stage candidates
  with ranks as (
    select job_id, person_id, job_candidate_stage_id, job_candidate_state_id,
        dense_rank() over(partition by job_id order by job_candidate_stage_id desc, job_candidate_state_id desc) as rnk -- find out max progress job canddiate 
    from sx.job_candidate 
    where job_candidate_state_id not in (45, 46, 47) -- recruiter, candidate, client reject
  ), 
  
  person_name as (
  select distinct person_id, full_name_display
  from sx.person p
  inner join sx.profile pf on p.profile_id = pf.id),
  
  public_co as (
   select c1.id as client_id, company_id, name as pub_co
    from sx.client c1
    inner join sx.company c2 on c1.company_id = c2.id
     where operating_status = 'public')

  select j.job_id, j.person_id, pub_co, 
    full_name_display as candidate, 
    --j.job_candidate_stage_id, 
    s1.name as job_candidate_stage, 
    --j.job_candidate_state_id, 
    s2.name as job_candidate_state, 
    jb.title as job_title, search_start_date
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join ranks m on m.job_id = j.job_id and m.person_id = j.person_id
  inner join person_name n on n.person_id = j.person_id
  inner join public_co c on c.client_id = jb.client_id
  inner join sx.job_candidate_stage s1 on s1.id = j.job_candidate_stage_id
  inner join sx.job_candidate_state s2 on s2.id = j.job_candidate_state_id
  where jb.job_status_id = 5 --open job
    and rnk = 1 
  order by 1,2;

-- COMMAND ----------

-- TEST
-- CEO request2: all open pub company jobs and maximum stage candidates
  with ranks as (
    select job_id, person_id, job_candidate_stage_id, job_candidate_state_id,
        dense_rank() over(partition by job_id order by job_candidate_stage_id desc, job_candidate_state_id desc) as rnk -- find out max progress job canddiate 
    from sx.job_candidate 
    --where job_candidate_state_id not in (45, 46, 47)
  ), 
  
  person_name as (
  select distinct person_id, full_name_display
  from sx.person p
  inner join sx.profile pf on p.profile_id = pf.id),
  
  public_co as (
   select c1.id as client_id, company_id, name as pub_co
    from sx.client c1
    inner join sx.company c2 on c1.company_id = c2.id
     where operating_status = 'public')

  select j.job_id, j.person_id, pub_co, 
    full_name_display as candidate, 
    --j.job_candidate_stage_id, 
    s1.name as job_candidate_stage, 
    --j.job_candidate_state_id, 
    s2.name as job_candidate_state, 
    jb.title as job_title, search_start_date
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join ranks m on m.job_id = j.job_id and m.person_id = j.person_id
  inner join person_name n on n.person_id = j.person_id
  inner join public_co c on c.client_id = jb.client_id
  inner join sx.job_candidate_stage s1 on s1.id = j.job_candidate_stage_id
  inner join sx.job_candidate_state s2 on s2.id = j.job_candidate_state_id
  where jb.job_status_id = 5 --open job
    and rnk = 1 and j.job_candidate_state_id not in (45, 46, 47) -- recruiter, candidate, client reject  
  order by 1,2;

-- COMMAND ----------

select * from sx.job where id in (12927, 12935);

-- COMMAND ----------

select * from sx.job_candidate where job_id = 12935 and job_candidate_state_id not in (45, 46, 47);

-- COMMAND ----------

select c1.id as client_id, company_id, name as pub_co
    from sx.client c1
    inner join sx.company c2 on c1.company_id = c2.id
     where operating_status = 'public' and c1.id = 313;

-- COMMAND ----------

select * from sx.cb_company where name like"%CloudCall%"

-- COMMAND ----------

select * from sx.company where id = 31278;

-- COMMAND ----------

-- all open company jobs and maximum stage candidates (include client reject candidates)
  with ranks as (
    select job_id, person_id, job_candidate_stage_id, job_candidate_state_id,
        dense_rank() over(partition by job_id order by job_candidate_stage_id desc, job_candidate_state_id desc) as rnk
    from sx.job_candidate 
  ), 
  
  person_name as (
  select distinct person_id, full_name_display
  from sx.person p
  inner join sx.profile pf on p.profile_id = pf.id),
  
  public_co as (
   select c1.id as client_id, company_id, name as pub_co
    from sx.client c1
    inner join sx.company c2 on c1.company_id = c2.id
     where operating_status = 'public')

  select j.job_id, j.person_id, pub_co, full_name_display as candidate, j.job_candidate_stage_id, j.job_candidate_state_id
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join ranks m on m.job_id = j.job_id and m.person_id = j.person_id
  inner join person_name n on n.person_id = j.person_id
  inner join public_co c on c.client_id = jb.client_id
  where jb.job_status_id = 5 --open job
    and rnk = 1
  order by 1,2;

-- COMMAND ----------

-- All the open jobs with maximum stage candidates
with max_progress as (
    select job_id, 
    max(job_candidate_stage_id) as max_job_candidate_stage,
    max(job_candidate_state_id) as max_job_candidate_state
    from sx.job_candidate
    group by job_id
  )

  select j.job_id, person_id, max_job_candidate_stage, max_job_candidate_state
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join max_progress m 
    on m.job_id = j.job_id and m.max_job_candidate_stage = j.job_candidate_stage_id and m.max_job_candidate_state = j.job_candidate_state_id
  where jb.job_status_id = 5 --open job 
  order by j.id desc, j.person_id
  ;

-- COMMAND ----------

with a as(
  select j.job_id, j.person_id, j.job_candidate_stage_id, j.job_candidate_state_id
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join (
    select job_id, person_id, job_candidate_stage_id, job_candidate_state_id,
        dense_rank() over(partition by job_id order by job_candidate_stage_id desc, job_candidate_state_id desc) as rnk
    from sx.job_candidate 
  ) m on m.job_id = j.job_id and m.person_id = j.person_id
  where jb.job_status_id = 5 --open job
    and rnk = 1),
  
  b as(
  select j.job_id, person_id, max_job_candidate_stage, max_job_candidate_state
  from sx.job_candidate j
  inner join sx.job jb on j.job_id = jb.id
  inner join (select job_id, 
    max(job_candidate_stage_id) as max_job_candidate_stage,
    max(job_candidate_state_id) as max_job_candidate_state
    from sx.job_candidate
    group by job_id) m 
      on m.job_id = j.job_id and m.max_job_candidate_stage = j.job_candidate_stage_id and m.max_job_candidate_state = j.job_candidate_state_id
  where jb.job_status_id = 5)
  
  select * from a
  except 
  select * from b
  order by 1, 2;

-- COMMAND ----------

select * from sx.job_candidate_stage;

-- COMMAND ----------

select * from sx.job_candidate_state;

-- COMMAND ----------

select * from sx.job_status;

-- COMMAND ----------

select c1.id as client_id, company_id, name
from sx.client c1
inner join sx.company c2 on c1.company_id = c2.id
where operating_status = 'public'-- Display all public client
select c1.id as client_id, company_id, name
from sx.client c1
inner join sx.company c2 on c1.company_id = c2.id
where operating_status = 'public';

-- COMMAND ----------

select * from sx.profile limit 10;

-- COMMAND ----------

select * from sx.employment_company limit 50;

-- COMMAND ----------

select * from sx.company where operating_status = 'public' limit 5;

-- COMMAND ----------

select * from sx.employment order by profile_id limit 50

-- COMMAND ----------

-- current employment with public company
select ec.id as employment_company_id, profile_id, employment_id, company_id, c.name as company_name, e.company_display as employment_company_display, title, start_date, end_date 
from sx.employment_company ec
inner join sx.company c on ec.company_id = c.id 
inner join sx.employment e on ec.employment_id = e.id 
where operating_status = 'public' and e.end_date is null
order by company_id, profile_id, employment_id;

-- COMMAND ----------

-- current employment with person name
select profile_id, company_display, title, start_date, end_date, p.id as person_id, full_name_display, p.location_display, linkedin_public_url 
from (select * from sx.employment where end_date is null) e 
left join sx.profile p on p.id = e.profile_id;

-- COMMAND ----------

select * from sx.person limit 10;

-- COMMAND ----------

select t.name, tt.name from sx.tag t inner join sx.tag_type tt on t.tag_type_id = tt.id where t.name = 'Current';

-- COMMAND ----------



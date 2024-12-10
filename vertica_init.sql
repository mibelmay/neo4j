-- Status
create table anchor_model.Status(status_id int, load_date timestamp);

-- Gender
create table anchor_model.Gender(gender_id int, load_date timestamp);

-- Region
create table anchor_model.Region(region_id int, load_date timestamp);

create table anchor_model.Region_Name(region_id int, name varchar, load_date timestamp)
    order by region_id, load_date;

-- User
create table anchor_model.User(user_id int, load_date timestamp);

create table anchor_model.User_Username(user_id int, username varchar, load_date timestamp)
    order by user_id, load_date;

create table anchor_model.User_Birthdate(user_id int, birthdate date, load_date timestamp)
    order by user_id, load_date;

create table anchor_model.User_Regdate(user_id int, reg_date date, load_date timestamp)
    order by user_id, load_date;

create table anchor_model.User_Gender(user_id int, gender_id int, load_date timestamp);

CREATE PROJECTION user_gender_proj_left AS SELECT * FROM anchor_model.User_Gender order by user_id
SEGMENTED BY HASH(user_id) ALL NODES;

CREATE PROJECTION user_gender_proj_right AS SELECT * FROM anchor_model.User_Gender order by gender_id
SEGMENTED BY HASH(gender_id) ALL NODES;

create table anchor_model.Lives(user_id int, region_id int, load_date timestamp);

CREATE PROJECTION lives_proj_left AS SELECT * FROM anchor_model.Lives order by user_id
SEGMENTED BY HASH(user_id) ALL NODES;

CREATE PROJECTION lives_proj_right AS SELECT * FROM anchor_model.Lives order by region_id
SEGMENTED BY HASH(region_id) ALL NODES;

-- Requests
create table anchor_model.Request(request_id int, load_date timestamp);

create table anchor_model.Request_Text(request_id int, text varchar, load_date timestamp)
    order by request_id, load_date;

create table anchor_model.Request_Date(request_id int, date date, load_date timestamp)
    order by request_id, load_date;

create table anchor_model.Requested(user_id int, request_id int, load_date timestamp);

CREATE PROJECTION requested_proj_left AS SELECT * FROM anchor_model.Requested order by user_id
SEGMENTED BY HASH(user_id) ALL NODES;

CREATE PROJECTION requested_proj_right AS SELECT * FROM anchor_model.Requested order by request_id
SEGMENTED BY HASH(request_id) ALL NODES;

-- Trends
create table anchor_model.Trend(trend_id int, load_date timestamp);

create table anchor_model.Trend_Name(trend_id int, name varchar, load_date timestamp)
    order by trend_id, load_date;

create table anchor_model.Trend_Status(trend_id int, status_id int, load_date timestamp)
    order by trend_id, load_date;

create table anchor_model.Contains(trend_id int, request_id int, load_date timestamp);

CREATE PROJECTION contains_proj_left AS SELECT * FROM anchor_model.Contains order by trend_id
SEGMENTED BY HASH(trend_id) ALL NODES;

CREATE PROJECTION contains_proj_right AS SELECT * FROM anchor_model.Contains order by request_id
SEGMENTED BY HASH(request_id) ALL NODES;

-- Schemas
select schema_id,
       schema_name,
       u.user_name as owner
from v_catalog.schemata s
join v_catalog.users u
     on s.schema_owner_id = u.user_id
order by schema_name;

-- Tables
select table_schema, table_name, create_time
from v_catalog.tables
where table_schema = 'anchor_model';
-- HIVE properties
set hive.exec.dynamic.partition.mode=nonstrict

-- CREATE HIVE DB
CREATE DATABASE IF NOT EXISTS VIDEOS;

-- CHANGE DATABASE
USE VIDEOS;

-- Create stagging table
CREATE EXTERNAL TABLE IF NOT EXISTS VIDEO_PLAY_STG(
channel_id INT,
creator_id INT,
disliked BOOLEAN,
geo_cd STRING,
liked BOOLEAN,
minutes_played INT, 
ts string,
user_id INT,
video_end_type TINYINT,
video_id INT)
PARTITIONED BY (process_time string)
STORED as ORC 
LOCATION "/user/edureka_735821/VideoPlayLake/Outbound/";

-- Load data to main table from stagging table
CREATE TABLE IF NOT EXISTS VIDEO_PLAY(
channel_id INT,
creator_id INT,
disliked BOOLEAN,
geo_cd STRING,
liked BOOLEAN,
minutes_played INT, 
ts string,
user_id INT,
video_end_type TINYINT,
video_id INT)
PARTITIONED BY (process_time string)
STORED as ORC;

-- Create table popular_channel
CREATE TABLE IF NOT EXISTS POPULAR_CHANNEL(
channel_id INT,
distinct_video_count BIGINT,
distict_user_id BIGINT,
process_time timestamp)
STORED as ORC;

-- Create table category_wise_duration
CREATE TABLE IF NOT EXISTS CATEGORY_DURATION(
user_type STRING,
duration BIGINT,
process_time timestamp)
STORED as ORC;

-- Create table connected_creator
CREATE TABLE IF NOT EXISTS CONNECTED_CREATOR(
creator_id INT,
user_count BIGINT,
process_time timestamp)
STORED as ORC;

-- Create table video_max_revenue
CREATE TABLE IF NOT EXISTS VIDEO_MAX_REVENUE(
video_id INT,
duration BIGINT,
process_time timestamp)
STORED as ORC;

-- Create table unsub_top_user
CREATE TABLE IF NOT EXISTS UNSUB_TOP_USER(
user_id INT,
duration BIGINT,
process_time timestamp)
STORED as ORC;

-- Create table video_play_stg_last_updated
CREATE TABLE IF NOT EXISTS VIDEO_PLAY_STG_LAST_UPDATED(
last_updated_time BIGINT)
STORED as ORC;

-- Create table video_play_last_updated

CREATE TABLE IF NOT EXISTS VIDEO_PLAY_LAST_UPDATED(
last_updated_time BIGINT)
STORED as ORC;

--Move data from stagging to main table
INSERT OVERWRITE TABLE VIDEO_PLAY 
partition(process_time)
select t1.* from video_play_stg t1 
inner join (select last_updated_time from video_play_last_updated) t2
inner join (select last_updated_time from video_play_stg_last_updated) t3
where unix_timestamp(t1.process_time, 'dd/MM/yyyy HH:mm:ss') between t2.last_updated_time and  t3.last_updated_time;

-- UPDATE video_play_last_updated
INSERT  OVERWRITE TABLE VIDEO_PLAY_LAST_UPDATED
SELECT max(unix_timestamp(process_time, 'dd/MM/yyyy HH:mm:ss')) FROM VIDEO_PLAY;

-- Table to store unique visitor count
CREATE TABLE IF NOT EXISTS visitor_uniq_cnt_735821(
ts timestamp,
visitor_cnt int,
constraint visitor_uniq_cnt_735821_PK PRIMARY KEY (ts)
);

-- Table to store daily sucess and failure response count
CREATE TABLE IF NOT EXISTS sucss_fail_cnt_dly_735821 (
dt DATE,
http200 int,
other int,
constraint sucss_fail_cnt_dly_735821_PK PRIMARY KEY (dt)
);

-- Table to store daily unique visitor count
CREATE TABLE IF NOT EXISTS visitor_uniq_cnt_dly_735821(
dt DATE,
visitor_cnt int,
constraint visitor_uniq_cnt_dly_735821_PK PRIMARY KEY (dt)
);

-- Table to state hourly traffic distribution
CREATE TABLE IF NOT EXISTS hourly_traffic_dist(
hours int,
visitor_cnt int);

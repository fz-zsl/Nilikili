-- drop all tables
drop table if exists user_like_danmu;
drop table if exists user_fav_video;
drop table if exists user_like_video;
drop table if exists user_coin_video;
drop table if exists user_watch_video;
drop table if exists user_follow;
drop view if exists danmu_active;
drop view if exists video_active_super;
drop view if exists video_active;
drop view if exists user_active;
drop table if exists danmu_info;
drop table if exists video_info;
drop table if exists user_info;

-- create tables
create table user_info (
    mid bigserial not null,
    name text not null,
    sex varchar(10),
    birthday varchar(10),
    level smallint not null default 0,
    sign text,
    identity varchar(5) not null default 'USER',
    pwd char(256), -- encrypted by SHA256
    qqid varchar(50),
    wxid varchar(50),
    coin int default 0,
    active boolean default true
);

create table video_info (
    bv varchar(25) not null,
    title text not null,
    ownMid bigint not null, -- owner's mid
    commitTime timestamp,
    revMid bigint, -- reviewer's mid
    reviewTime timestamp,
    publicTime timestamp,
    duration float8, -- in seconds
    descr text, -- description
    active boolean default true
-- only means not deleted, may not be visible
);

create table danmu_info (
    danmu_id bigserial not null,
    bv varchar(25) not null,
    senderMid bigint not null,
    showTime float8 not null,
    -- the display time from the start of video (in seconds)
    content text,
    postTime timestamp,
    active boolean default true
);

create table user_follow (
    star_mid bigint not null,
    fan_mid bigint not null
);

create table user_watch_video (
    mid bigint not null,
    bv varchar(25) not null,
    lastpos float8 not null -- last watch time stamp in seconds
);

create table user_coin_video (
    mid bigint not null,
    bv varchar(25) not null
);

create table user_like_video (
    mid bigint not null,
    bv varchar(25) not null
);

create table user_fav_video (
    mid bigint not null,
    bv varchar(25) not null
);

create table user_like_danmu (
    danmu_id bigint not null,
    mid bigint not null
);

-- keys and constraints are added later


-- drop all functions
drop function if exists verify_auth;
drop function if exists verify_video_req;
-- drop function if exists user_reg_check;
drop function if exists user_reg_sustc;
drop function if exists user_del_sustc;
drop function if exists add_follow;
drop function if exists get_user_info;
drop function if exists generate_unique_bv;
drop function if exists post_video;
drop function if exists del_video;
drop function if exists update_video;
drop function if exists search_video;
drop function if exists get_avg_view_rate;
drop function if exists get_hotspot;
drop function if exists rev_video;
drop function if exists coin_video;
drop function if exists like_video;
drop function if exists fav_video;
drop function if exists send_danmu;
drop function if exists display_danmu;
drop function if exists like_danmu;
drop function if exists recommend_next_video;
drop function if exists general_recommendations;
drop function if exists recommend_video_for_user;
drop function if exists recommend_friends;
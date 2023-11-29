drop table if exists user_like_danmu;
drop table if exists user_fav_video;
drop table if exists user_like_video;
drop table if exists user_coin_video;
drop table if exists user_watch_video;
drop table if exists user_follow;
drop table if exists danmu_info;
drop table if exists video_info;
drop table if exists user_info;

create table user_info (
    mid bigint not null,
    name text not null,
    sex varchar(10),
    birthday date,
    level smallint not null default 0,
    sign text,
    identity varchar(10) not null,
    pwd char(256), -- encrypted by SHA256
    qqid varchar(13),
    wxid varchar(50),
    coin numeric(10,2),
    active boolean default true,
    constraint mid_pk primary key (mid)
);

create table video_info (
    bv varchar(15) not null,
    title text not null,
    ownMid bigint not null, -- owner's mid
    commitTime timestamp,
    revMid bigint, -- reviewer's mid
    reviewTime timestamp,
    publicTime timestamp,
    duration bigint, -- in seconds
    descr text, -- description
    active boolean default true,
    constraint bv_pk primary key (bv)
);

create table danmu_info (
    danmu_id bigserial not null,
    bv varchar(15) not null,
    senderMid bigint not null,
    showtime float8 not null,
    -- the display time from the start of video (in seconds)
    content text,
    posttime timestamp,
    active boolean default true,
    constraint danmu_id_pk primary key (danmu_id),
    constraint mid_fk foreign key (senderMid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_follow (
    star_mid bigint not null,
    fan_mid bigint not null,
    constraint star_fk foreign key (star_mid) references user_info(mid),
    constraint fan_fk foreign key (fan_mid) references user_info(mid)
);

create table user_watch_video (
    mid bigint not null,
    bv varchar(15) not null,
    lastpos bigint not null, -- last watch time stamp in seconds
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_coin_video (
    mid bigint not null,
    bv varchar(25) not null,
    -- given int, -- the number of coins given by the user
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_like_video (
    mid bigint not null,
    bv varchar(15) not null,
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_fav_video (
    mid bigint not null,
    bv varchar(15) not null,
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_like_danmu (
    danmu_id bigint not null,
    mid bigint not null,
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint did_fk foreign key (danmu_id) references danmu_info(danmu_id)
);
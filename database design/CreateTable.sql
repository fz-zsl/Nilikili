create table user_info (
    mid serial not null,
    name varchar(50) not null,
    sex varchar(10),
    birthday date,
    level int not null default 0,
    sign text,
    indentity text(10) not null,
    pwd char(256), -- encrypted by SHA256
    qqid bigint,
    wxid varchar(50),
    coin numeric(10,2),
    constraint mid_pk primary key (mid)
);

create table video_info (
    bv char(12) not null,
    title text not null,
    ownMid int not null, -- owner's mid
    commitTime timestamp,
    revMid int, -- reviewer's mid
    reviewTime timestamp,
    publicTime timestamp,
    duration int, -- in seconds
    descr text, -- description
);

create table danmu_info (
    danmu_id int not null auto_increment,
    bv char(12) not null,
    senderMid int not null,
    showtime int not null,
        -- the display time from the start of video (in seconds)
    content text,
    posttime timestamp,
    constraint mid_fk foreign key (senderMid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_follow (
    star_mid int not null,
    fan_mid int not null,
    constraint star_fk foreign key (star_mid) references user_info(mid),
    constraint fan_fk foreign key (fan_mid) references user_info(mid)
);

create table user_watch_video (
    mid int not null,
    bv char(12) not null,
    lastpos int not null, -- last watch time stamp in seconds
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_coin_video (
    mid int not null,
    bv char(12) not null,
    given int, -- the number of coins given by the user
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_like_video (
    mid int not null,
    bv char(12) not null,
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_fav_video (
    mid int not null,
    bv char(12) not null,
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create user_like_danmu (
    did int not null, -- danmu_id
    mid int not null,
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint did_fk foreign key (did) references danmu_info(did)
);
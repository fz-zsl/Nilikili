package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

	/**
	 * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
	 * <p>
	 * Marking a field with {@link Autowired} annotation enables our framework to automatically
	 * provide you a well-configured instance of {@link DataSource}.
	 * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
	 */
	@Autowired
	private DataSource dataSource;

	/**
	 * Acknowledges the authors of this project.
	 *
	 * @return a list of group members' student-id
	 */
	@Override
	public List<Integer> getGroupMembers() {
		return List.of(12212232);
	}
	/**
	 * Imports data to an empty database.
	 * Invalid data will not be provided.
	 *
	 * @param danmuRecords danmu records parsed from csv
	 * @param userRecords  user records parsed from csv
	 * @param videoRecords video records parsed from csv
	 */
	@Override
	public void importData(
		List<DanmuRecord> danmuRecords,
		List<UserRecord> userRecords,
		List<VideoRecord> videoRecords
	) { // TODO: ask the default value of coin and level
		String createSQL = """

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
    birthday date,
    level smallint not null default 0,
    sign text,
    identity varchar(5) not null default 'USER',
    pwd char(256), -- encrypted by SHA256
    qqid varchar(20),
    wxid varchar(30),
    coin int default 0,
    active boolean default true,
    constraint mid_pk primary key (mid),
    constraint sex_valid check (sex in ('MALE', 'FEMALE', 'UNKNOWN')),
    constraint identity_valid check (identity in ('USER', 'SUPER')),
    constraint coin_non_neg check (coin >= 0)
);

create table video_info (
    bv varchar(15) not null,
    title text not null,
    ownMid bigint not null, -- owner's mid
    commitTime timestamp,
    revMid bigint, -- reviewer's mid
    reviewTime timestamp,
    publicTime timestamp,
    duration float8, -- in seconds
    descr text, -- description
    active boolean default true,
        -- only means not deleted, may not be visible
    constraint bv_pk primary key (bv)
);

create table danmu_info (
    danmu_id bigserial not null,
    bv varchar(15) not null,
    senderMid bigint not null,
    showTime float8 not null,
        -- the display time from the start of video (in seconds)
    content text,
    postTime timestamp,
    active boolean default true,
	constraint danmu_id_pk primary key (danmu_id),
    constraint mid_fk foreign key (senderMid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_follow (
    star_mid bigint not null,
    fan_mid bigint not null,
    constraint user_follow_pk primary key (star_mid, fan_mid),
    constraint star_fk foreign key (star_mid) references user_info(mid),
    constraint fan_fk foreign key (fan_mid) references user_info(mid)
);

create table user_watch_video (
    mid bigint not null,
    bv varchar(15) not null,
    lastpos float8 not null, -- last watch time stamp in seconds
    constraint user_watch_video_pk primary key (mid, bv),
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_coin_video (
    mid bigint not null,
    bv varchar(25) not null,
    constraint user_coin_video_pk primary key (mid, bv),
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_like_video (
    mid bigint not null,
    bv varchar(15) not null,
    constraint user_like_video_pk primary key (mid, bv),
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_fav_video (
    mid bigint not null,
    bv varchar(15) not null,
    constraint user_fav_video_pk primary key (mid, bv),
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint bv_fk foreign key (bv) references video_info(bv)
);

create table user_like_danmu (
    danmu_id bigint not null,
    mid bigint not null,
    constraint user_like_danmu_pk primary key (danmu_id, mid),
    constraint mid_fk foreign key (mid) references user_info(mid),
    constraint did_fk foreign key (danmu_id) references danmu_info(danmu_id)
);
		""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(createSQL)) {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		// load user_info
		String insertUserInfoSQL = """
insert into user_info (mid, name, sex, birthday, level, sign, identity, pwd, qqid, wxid, coin)
	values (?, ?, ?, to_date(?, ?), ?, ?, ?, digest(?, 'sha256'), ?, ?, ?);
			""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserInfoSQL)) {
			for (UserRecord userRecord : userRecords) {
				stmt.setLong(1, userRecord.getMid());
				stmt.setString(2, userRecord.getName());
				String sexString = userRecord.getSex();if (sexString.equals("M") || sexString.equals("男") || sexString.equals("♂")) {
					sexString = "MALE";
				}
				else if (sexString.equals("F") || sexString.equals("女") || sexString.equals("♀")) {
					sexString = "FEMALE";
				}
				else {
					sexString = "UNKNOWN";
				}
				stmt.setString(3, sexString);
				String birthday = userRecord.getBirthday();
				stmt.setString(4, userRecord.getBirthday());
				if (birthday == null || birthday.isEmpty()) {
					stmt.setString(5, "MM-DD");
				}
				else if (birthday.matches("^[0-9]+月[0-9]+日$")) {
					stmt.setString(5, "MM月DD日");
				}
				else {
					int arg1 = Integer.parseInt(birthday.substring(0, birthday.indexOf('-')));
					if (arg1 > 12) {
						stmt.setString(5, "DD-MM");
					}
					else {
						stmt.setString(5, "MM-DD");
					}
				}
				stmt.setShort(6, userRecord.getLevel());
				stmt.setString(7, userRecord.getSign());
				stmt.setString(8, userRecord.getIdentity().name().equals("USER") ? "USER" : "SUPER");
				stmt.setString(9, userRecord.getPassword());
				stmt.setString(10, userRecord.getQq());
				stmt.setString(11, userRecord.getWechat());
				stmt.setInt(12, userRecord.getCoin());
				stmt.addBatch();
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user records, " + e.getMessage());
		}

		// load video_info
		String insertVideoInfoSQL = """
insert into video_info (bv, title, ownMid, commitTime, revMid, reviewTime, publicTime, duration, descr)
	values (?, ?, ?, ?, ?, ?, ?, ?, ?);
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(insertVideoInfoSQL)) {
			for (VideoRecord videoRecord : videoRecords) {
				stmt.setString(1, videoRecord.getBv());
				stmt.setString(2, videoRecord.getTitle());
				stmt.setLong(3, videoRecord.getOwnerMid());
				stmt.setTimestamp(4, videoRecord.getCommitTime());
				stmt.setLong(5, videoRecord.getReviewer());
				stmt.setTimestamp(6, videoRecord.getReviewTime());
				stmt.setTimestamp(7, videoRecord.getPublicTime());
				stmt.setFloat(8, videoRecord.getDuration());
				stmt.setString(9, videoRecord.getDescription());
				stmt.addBatch();
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert video records, " + e.getMessage());
		}

		// load danmu_info
		String insertDanmuInfoSQL = """
insert into danmu_info (danmu_id, bv, senderMid, showtime, content, posttime)
	values (?, ?, ?, ?, ?, ?);
		""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertDanmuInfoSQL)) {
			long danmuCnt = 0;
			for (DanmuRecord danmuRecord : danmuRecords) {
				danmuRecord.setDanmuId(++danmuCnt);
				stmt.setLong(1, danmuCnt);
				stmt.setString(2, danmuRecord.getBv());
				stmt.setLong(3, danmuRecord.getMid());
				stmt.setFloat(4, danmuRecord.getTime());
				stmt.setString(5, danmuRecord.getContent());
				stmt.setTimestamp(6, danmuRecord.getPostTime());
				stmt.addBatch();
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert danmu records, " + e.getMessage());
		}

		// load user_follow
		String insertUserFollowSQL = "insert into user_follow (star_mid, fan_mid) values (?, ?);";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
			for (UserRecord userRecord : userRecords) {
				stmt.setLong(2, userRecord.getMid());
				for (Long starMid : userRecord.getFollowing()) {
					stmt.setLong(1, starMid);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
		}

		// load user_watch_video
		String insertUserWatchVideoSQL = "insert into user_watch_video (mid, bv, lastpos) values (?, ?, ?);";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserWatchVideoSQL)) {
			for (VideoRecord videoRecord : videoRecords) {
				stmt.setString(2, videoRecord.getBv());
				int viewerCnt = videoRecord.getViewerMids().length;
				for (int i = 0; i < viewerCnt; i++) {
					stmt.setLong(1, videoRecord.getViewerMids()[i]);
					stmt.setFloat(3, videoRecord.getViewTime()[i]);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_watch_video records, " + e.getMessage());
		}

		// load user_coin_video
		String insertUserCoinVideoSQL = "insert into user_coin_video (mid, bv) values (?, ?);";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserCoinVideoSQL)) {
			for (VideoRecord videoRecord : videoRecords) {
				stmt.setString(2, videoRecord.getBv());
				for (Long mid : videoRecord.getCoin()) {
					stmt.setLong(1, mid);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_coin_video records, " + e.getMessage());
		}

		// load user_like_video
		String insertUserLikeVideoSQL = "insert into user_like_video (mid, bv) values (?, ?);";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserLikeVideoSQL)) {
			for (VideoRecord videoRecord : videoRecords) {
				stmt.setString(2, videoRecord.getBv());
				for (Long mid : videoRecord.getLike()) {
					stmt.setLong(1, mid);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_like_video records, " + e.getMessage());
		}

		// load user_fav_video
		String insertUserFavVideoSQL = "insert into user_fav_video (mid, bv) values (?, ?);";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserFavVideoSQL)) {
			for (VideoRecord videoRecord : videoRecords) {
				stmt.setString(2, videoRecord.getBv());
				for (Long mid : videoRecord.getFavorite()) {
					stmt.setLong(1, mid);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_fav_video records, " + e.getMessage());
		}

		// load user_like_danmu
		String insertUserLikeDanmuSQL = "insert into user_like_danmu (danmu_id, mid) values (?, ?);";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserLikeDanmuSQL)) {
			for (DanmuRecord danmuRecord : danmuRecords) {
				stmt.setLong(1, danmuRecord.getDanmuId());
				for (Long mid : danmuRecord.getLikedBy()) {
					stmt.setLong(2, mid);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_like_danmu records, " + e.getMessage());
		}



		String createFunctions = """

				-- create views
create or replace view user_active as
	select * from user_info where active = true;

create or replace view video_active as
	select * from video_info where active = true and revMid is not null
		and (publicTime is null or publicTime <= now());
	
create or replace view video_active_super as
	select * from video_info where active = true;

create or replace view danmu_active as
	select * from danmu_info where active = true;



-- function for VerifyAuth
create extension if not exists pgcrypto;

create or replace function verify_auth(
    _mid bigint,
    _pwd text,
    _qqid text,
    _wxid text
)
    returns bigint as $$
        declare
            mid_mid bigint;
            qqid_mid bigint;
            wxid_mid bigint;
    begin
        mid_mid := (
            select mid from user_active
                where user_active.mid = _mid
                    and user_active.pwd = digest(_pwd, 'sha256')
        );
        qqid_mid := (
            select mid from user_active
                where user_active.qqid = _qqid
        );
        wxid_mid := (
            select mid from user_active
                where user_active.wxid = _wxid
        );
        if _qqid is not null and _wxid is not null
            and qqid_mid <> wxid_mid then
            raise notice 'OIDC via QQ and WeChat contradicts.';
            return -1;
        end if;
        if mid_mid is null and qqid_mid is null and wxid_mid is null then
            raise notice 'Authentication failed.';
            return -1;
        end if;
        if mid_mid is not null then
            return mid_mid;
        end if;
        if qqid_mid is not null then
            return qqid_mid;
        end if;
        if wxid_mid is not null then
            return wxid_mid;
        end if;
    end $$ language plpgsql;



-- function for VerifyVideoReq
create or replace function verify_video_req(
    _title text,
    _duration float8,
    _publicTime timestamp,
    _auth_mid bigint
)
    returns boolean as $$
    begin
        if _title is null or _title = '' then
            raise notice 'Title is null or empty.';
            return false;
        end if;
        if _duration < 10 then
            raise notice 'Duration is less than 10 seconds.';
            return false;
        end if;
        if _publicTime is not null and _publicTime < now() then
            raise notice 'Publish time before current time.';
            return false;
        end if;
        if (select count(*) from video_active_super
            where video_active_super.title = _title
                and video_active_super.ownMid = _auth_mid
            ) > 0 then
            raise notice 'Title already exists.';
            return false;
        end if;
        return true;
    end $$ language plpgsql;

-- functions for UserServiceImpl
create or replace function user_reg_check()
    returns trigger as $$
    begin
        if new.name is null or new.name = '' then
            raise notice 'Name cannot be null or empty.';
            return NULL;
        end if;
        if new.pwd is null or new.pwd = '' then
            raise notice 'Password cannot be null or empty.';
            return NULL;
        end if;
        new.pwd := digest(new.pwd, 'sha256');
        if new.sex is null then
            raise notice 'Sex cannot be null.';
            return NULL;
        end if;
        if upper(new.sex) = 'M' or new.sex = '男'
            or upper(new.sex) = 'MALE' or new.sex = '♂' then
            new.sex := 'MALE';
        elsif upper(new.sex) = 'F' or new.sex = '女'
            or upper(new.sex) = 'FEMALE' or new.sex = '♀' then
            new.sex := 'FEMALE';
        else
            new.sex := 'UNKNOWN';
        end if;
        if (select count(*) from user_active where user_active.name = new.name) > 0 then
            raise notice 'Username used.';
            return NULL;
        end if;
        if (select count(*) from user_active where user_active.qqid = new.qqid) > 0 then
            raise notice 'QQ used.';
            return NULL;
        end if;
        if (select count(*) from user_active where user_active.wxid = new.wxid) > 0 then
            raise notice 'WeChat used.';
            return NULL;
        end if;
        return new;
    end $$ language plpgsql;

create or replace function user_reg(
    _name text,
    _sex text,
    _birthday text,
    _sign text,
    _pwd text,
    _qqid varchar(20),
    _wxid varchar(30)
)
    returns bigint as $$
    declare
        id bigint;
    begin
        begin
	        insert into user_info (name, sex, birthday, sign, pwd, qqid, wxid)
	            values (_name, _sex, _birthday, _sign, _pwd, _qqid, _wxid);
        exception when others then
            raise notice 'User registration failed.';
            return -1;
        end;
        id := (
			select mid from user_active
				where user_active.name = _name
		);
		return id;
    end $$ language plpgsql;

create or replace function user_del(
    auth_mid bigint,
    auth_pwd text,
    auth_qqid text,
    auth_wxid text,
    _mid bigint
)
    returns boolean as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from user_active where user_active.mid = _mid) = 0 then
            raise notice 'User not found.';
            return false;
        end if;
        if auth_mid = _mid or
            ((select identity from user_active where user_active.mid = auth_mid) = 'SUPER')
            and ((select identity from user_active where user_active.mid = _mid) = 'USER') then
            update user_info set active = false where user_info.mid = _mid;
            update video_info set active = false where video_info.ownMid = _mid;
            update danmu_info set active = false where bv in (
                select bv from video_info where video_info.ownMid = _mid
            );
            update danmu_info set active = false where senderMid = _mid;
            return true;
        end if;
    end $$ language plpgsql;

create or replace function add_follow(
    auth_mid bigint,
    auth_pwd text,
    auth_qqid text,
    auth_wxid text,
    followee_mid bigint
)
    returns boolean as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from user_active where user_active.mid = followee_mid) = 0 then
			raise notice 'Followee not found.';
			return false;
		end if;
		begin
			insert into user_follow (star_mid, fan_mid) values (followee_mid, auth_mid);
		exception when others then
			raise notice 'Followee already followed.';
			return false;
		end;
		return true;
	end $$ language plpgsql;



-- functions for VideoServiceImpl
create or replace function generate_unique_bv() returns text as $$
	declare
        new_uuid text;
	begin
	    loop
	        new_uuid := substring(gen_random_uuid()::text FROM 1 FOR 10);
	        if not exists (select 1 from video_info where bv = new_uuid) then
	            return 'BV' || new_uuid;
	        end if;
	    end loop;
	end $$ language plpgsql;

create or replace function post_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _title text,
    _descr text,
    _duration float8,
    _publicTime timestamp
)
    returns varchar(15) as $$
    declare
        bv text;
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return null;
        end if;
        if not verify_video_req(_title, _duration, _publicTime, auth_mid) then
            raise notice 'Video verification failed.';
            return null;
        end if;
        bv := generate_unique_bv();
        insert into video_info (bv, title, ownMid, commitTime, publicTime, duration, descr)
            values (bv, _title, auth_mid, now(), _publicTime, _duration, _descr);
        return bv;
    end $$ language plpgsql;

create or replace function del_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15)
)
    returns boolean as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return false;
        end if;
        if auth_mid = (select ownMid from video_active_super where bv = _bv) or
            ((select identity from user_active where user_active.mid = auth_mid) = 'SUPER') then
            update video_info set active = false where bv = _bv;
            update danmu_info set active = false where bv = _bv;
            return true;
        end if;
        return false;
    end $$ language plpgsql;

create or replace function update_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15),
    _title text,
    _descr text,
    _duration float8,
    _publicTime timestamp
)
    returns boolean as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from video_active_super
            where bv = _bv and ownMid = auth_mid and duration = _duration) = 0 then
            raise notice 'Video not found / duration changed.';
            return false;
        end if;
        if not verify_video_req(_title, _duration, _publicTime, auth_mid) then
            raise notice 'Video verification failed.';
            return false;
        end if;
        if (select count(*) from video_active_super
            where bv = _bv and title = _title and descr = _descr
                and publicTime = _publicTime) > 0 then
            raise notice 'Nothing changes.';
            return false;
        end if;
        update video_info set
            title = _title,
            descr = _descr,
            duration = _duration,
            publicTime = _publicTime
            where bv = _bv;
        return true;
    end $$ language plpgsql;

create or replace function search_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    keywords text,
    page_size int,
    page_num int
)
    returns table(bv text) as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise exception 'Authentication failed.';
        end if;
        if keywords is null or keywords = '' then
            raise exception 'No keyword provided.';
        end if;
        if page_size <= 0 or page_num <= 0 then
            raise exception 'Invalid page size or page num.';
        end if;
        return query
        select watch_cnt.bv from
            (select bv, count(*) as cnt
                from user_watch_video group by bv
            ) as watch_cnt
        join
            (select bv, revMid, publicTime, ownMid, (
                array_length(regexp_matches(tmp1.title, word, 'g'), 1) +
                array_length(regexp_matches(tmp1.descr, word, 'g'), 1) +
                array_length(regexp_matches(tmp1.name, word, 'g'), 1)
            ) as relevance
                from (
                    (select bv, title, descr, ownMid, name, revMid, publicTime
                        from (video_active_super join user_active on ownMid = mid)) tmp2
                    cross join (SELECT regexp_split_to_table(keywords, E'\\\\\\\\s+') as word) tmp3
                )
            as tmp1) tmp4
        on watch_cnt.bv = tmp4.bv
        where tmp4.revMid is not null and tmp4.publicTime < now()
            or tmp4.ownMid = auth_mid
            or (select identity from user_active where mid = auth_mid) = 'SUPER'
        order by relevance desc, cnt desc limit page_size offset ((page_num - 1) * page_size);
    end $$ language plpgsql;

create or replace function get_avg_view_rate(_bv varchar(15))
    returns double precision as $$
    declare
        _cnt int;
        _avg double precision;
    begin
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return -1;
        end if;
        select count(*), avg(lastpos) into _cnt, _avg from user_watch_video where bv = _bv;
        if _cnt = 0 then
            raise notice 'No one has watched this video.';
            return -1;
        end if;
        return _avg / (select duration from video_active_super where bv = _bv);
    end $$ language plpgsql;

create or replace function get_hotspot(_bv varchar(15))
    returns table(chunkId int) as $$
    begin
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise exception 'Video not found.';
        end if;
        return query
        select chunkId from (
            select chunkId, cnt, max(cnt) over() as maxx from (
                select floor(showtime / 10) as chunkId, count(*) as cnt
                    from danmu_info where bv = _bv group by chunkId
            ) as danmu_cnt
        ) as hotspot where cnt = maxx and maxx <> 0;
    end $$ language plpgsql;

create or replace function rev_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15)
)
    returns boolean as $$
    declare
        _ownMid bigint;
        _revMid bigint;
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return false;
        end if;
        if (select identity from user_active where mid = auth_mid) = 'USER' then
            raise notice 'Insufficient permission.';
            return false;
        end if;
        select ownMid, revMid into _ownMid, _revMid from video_active_super where bv = _bv;
        if _ownMid = auth_mid then
            raise notice 'Cannot review own video.';
            return false;
        end if;
        if _revMid is not null then
            raise notice 'Video has been reviewed.';
            return false;
        end if;
        begin
            update video_info
                set revMid = auth_mid, reviewTime = now()
                where bv = _bv;
        exception when others then
            raise notice 'Review failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;

create or replace function coin_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15)
)
    returns boolean as $$
    declare
        _ownMid bigint;
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return false;
        end if;
        if (select identity from user_active where mid = auth_mid) = 'USER' then
            if (select count(*) from video_active where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return false;
            end if;
        else
            if (select count(*) from video_active_super where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return false;
            end if;
        end if;
        _ownMid := (select ownMid from video_active_super where bv = _bv);
        if (_ownMid = auth_mid) then
            raise notice 'Cannot coin your own video';
            return false;
        end if;
        begin
            insert into user_coin_video (mid, bv) values (auth_mid, _bv);
            update user_info set coin = coin - 1 where mid = auth_mid;
            update user_info set coin = coin + 1 where mid = (
                select ownMid from video_active_super where bv = _bv
            );
        exception when others then
            raise notice 'Coin failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;

create or replace function like_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15)
)
    returns boolean as $$
    declare
        _ownMid bigint;
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return false;
        end if;
        if (select identity from user_active where mid = auth_mid) = 'USER' then
            if (select count(*) from video_active where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return false;
            end if;
        else
            if (select count(*) from video_active_super where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return false;
            end if;
        end if;
        _ownMid := (select ownMid from video_active_super where bv = _bv);
        if (_ownMid = auth_mid) then
            raise notice 'Cannot coin your own video';
            return false;
        end if;
        begin
            insert into user_like_video (mid, bv)
                values (auth_mid, _bv);
        exception when others then
            raise notice 'Like failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;

				create or replace function fav_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15)
				)
    returns boolean as $$
    declare
        _ownMid bigint;
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return false;
        end if;
        if (select identity from user_active where mid = auth_mid) = 'USER' then
            if (select count(*) from video_active where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return false;
            end if;
        else
            if (select count(*) from video_active_super where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return false;
            end if;
        end if;
        _ownMid := (select ownMid from video_active_super where bv = _bv);
        if (_ownMid = auth_mid) then
            raise notice 'Cannot collect your own video';
            return false;
        end if;
        begin
            insert into user_fav_video (mid, bv)
                values (auth_mid, _bv);
        exception when others then
            raise notice 'Collection failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;



-- functions for DanmuServiceImpl
create or replace function send_danmu (
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _bv varchar(15),
    _content text,
    show_time float8
)
    returns bigint as $$
    declare
        _danmu_id bigint;
        public_time timestamp;
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return -1;
        end if;
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise notice 'Video not found.';
            return -1;
        end if;
        if (select identity from user_active where mid = auth_mid) = 'USER' then
            if (select count(*) from video_active where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return -1;
            end if;
        else
            if (select count(*) from video_active_super where bv = _bv) = 0 then
                raise notice 'Video doesn''t exist.';
                return -1;
            end if;
        end if;
        if _content is null or _content = '' then
            raise notice 'Content is empty.';
            return -1;
        end if;
        public_time := (select publicTime from video_active_super where bv = _bv);
        if public_time is not null and public_time > now() then
            raise notice 'Public time is invalid.';
            return -1;
        end if;
        if (select count(*) from user_watch_video
            where mid = auth_mid and bv = _bv) = 0 then
            raise notice 'You should watch the video first.';
            return -1;
        end if;
        begin
            insert into danmu_info (bv, senderMid, showtime, content, postTime)
                values (_bv, auth_mid, show_time, _content, now());
            select danmu_id into _danmu_id from danmu_info
                where bv = _bv and senderMid = auth_mid and showtime = show_time;
        exception when others then
                raise notice 'Send danmu failed.';
                return -1;
        end;
        return _danmu_id;
    end $$ language plpgsql;

create or replace function display_danmu (
    _bv text,
    start_time float8,
    end_time float8,
    _filter boolean
)
    returns table(danmu_id bigint) as $$
    begin
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise exception 'Video not found.';
        end if;
        if (start_time > end_time or start_time < 0
            or end_time > (select duration from video_active_super where bv = _bv)) then
            raise exception 'Time is invalid.';
        end if;
        if _filter then
            return query
            with allDanmu as (
                select danmu_id, content, postTime from danmu_active
                    where bv = _bv and showTime between start_time and end_time
            )
            select danmu_id from (
                select danmu_id, postTime,
                    min(posttime) over(partition by content) as firstPosted
                from allDanmu
            ) as DPP where postTime = firstPosted;
        else
            return query
            select danmu_id from danmu_active
                where bv = _bv and showTime between start_time and end_time;
        end if;
    end $$ language plpgsql;

create or replace function like_danmu (
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    _danmu_id bigint
)
    returns boolean as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise notice 'Authentication failed.';
            return false;
        end if;
        if (select count(*) from danmu_active where danmu_id = _danmu_id) = 0 then
            raise notice 'Danmu not found.';
            return false;
        end if;
        begin
            insert into user_like_danmu(danmu_id, mid)
                values (_danmu_id, auth_mid);
        exception when others then
            raise notice 'Like danmu failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;



-- functions for RecommenderServiceImpl
create or replace function recommend_next_video (_bv text)
    returns table(bv text) as $$
    begin
        if (select count(*) from video_active_super where bv = _bv) = 0 then
            raise exception 'Video not found.';
        end if;
        return query
        select count(bv) as cnt from (
	        (select mid from user_watch_video where bv = _bv) as watchedBv
            join user_watch_video on watchedBv.mid = user_watch_video.mid)
		    group by bv order by cnt desc limit 5;
    end $$ language plpgsql;

create or replace function general_recommendations (
    page_size int,
    page_num int
)
    returns table(bv text) as $$
    declare
        auxCnt int;
    begin
        if page_size <= 0 or page_num <= 0 then
            raise notice 'Page size or page number is invalid.';
            return;
        end if;
        return query
        select bv, (
            case
                when (select count(*) as cnt into auxCnt from user_watch_video where bv = video_active_super.bv) = 0
                    then 0
                else ((
                    (select count(*) from user_like_video where user_like_video.bv = video_active_super.bv) +
                    (select count(*) from user_coin_video where user_coin_video.bv = video_active_super.bv) +
                    (select count(*) from user_fav_video where user_fav_video.bv = video_active_super.bv) +
                    (select count(*) from danmu_info where danmu_info.bv = video_active_super.bv) +
                    (select sum(lastpos) from user_watch_video where user_watch_video.bv = video_active_super.bv)
                        / video_active_super.duration
                    ) / auxCnt)
            end) as score from video_active_super
            limit page_size offset (page_num - 1) * page_size;
    end; $$ language plpgsql;

create or replace function recommend_video_for_user (
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    page_size int,
    page_num int
)
    returns table(bv text) as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise exception 'Authentication failed.';
        end if;
        if page_size <= 0 or page_num <= 0 then
            raise exception 'Page size or page number is invalid.';
        end if;
        return query
        with friends as (
            select star_mid from user_follow where fan_mid = auth_mid
            intersect
            select fan_mid from user_follow where star_mid = auth_mid
        )
        select validBv.bv from (
            select bv, count(*) as cnt from user_watch_video
                where mid in (select * from friends) and bv not in (
                    select bv from user_watch_video where mid = auth_mid
                ) group by bv) validBv
            join video_active_super on video_active_super.bv = validBv.bv
            join user_active on user_active.mid = video_active_super.ownMid
            where (revMid is not null and publicTime <= now()
                or (select identity from user_active where mid = auth_mid) = 'SUPER')
            order by cnt desc, level desc, publicTime desc
            limit page_size offset (page_num - 1) * page_size;
    end $$ language plpgsql;

create or replace function recommend_friends (
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(20),
    auth_wxid varchar(30),
    page_size int,
    page_num int
)
    returns table(bv text) as $$
    begin
        if (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid)) < 0 then
            raise exception 'Authentication failed.';
        end if;
        if page_size <= 0 or page_num <= 0 then
            raise exception 'Page size or page number is invalid.';
        end if;
        return query
        with newFriends as (
            select fan_mid as mid, count(fan_mid) as cnt from
            (select star_mid from user_follow where fan_mid = auth_mid) myFollowings
                join user_follow on myFollowings.star_mid = user_follow.star_mid
            where fan_mid not in (select star_mid from user_follow where fan_mid = auth_mid)
                and fan_mid <> auth_mid
            group by fan_mid
        )
        select user_active.mid
            from newFriends join user_active
                on newFriends.mid = user_active.mid
            order by cnt desc, level desc limit page_size offset (page_num - 1) * page_size;
    end; $$ language plpgsql;
		""";
		try (Connection conn = dataSource.getConnection();
				PreparedStatement stmt = conn.prepareStatement(createFunctions)) {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		String addUserTrigger = """
create or replace trigger user_reg_trigger
    before insert on user_info for each row
    execute procedure user_reg_check();
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(addUserTrigger)) {
			stmt.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The following code is just a quick example of using jdbc datasource.
	 * Practically, the code interacts with database is usually written in a DAO layer.
	 * Reference: <a href="https://www.baeldung.com/java-dao-pattern">Data Access Object pattern</a>
	 */

	@Override
	public void truncate() {
		// You can use the default truncate script provided by us in most cases,
		// but if it doesn't work properly, you may need to modify it.

		String sql = """
DO $$
DECLARE
	tables CURSOR FOR
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public';
BEGIN
	FOR t IN tables
	LOOP
		EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';
	END LOOP;
END $$;
		""";

		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(sql)) {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Integer sum(int a, int b) {
		String sql = "SELECT ?+?";

		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(sql)) {
			stmt.setInt(1, a);
			stmt.setInt(2, b);
			log.info("SQL: {}", stmt);

			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}

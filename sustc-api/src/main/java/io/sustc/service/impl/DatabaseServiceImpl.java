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
    constraint identity_valid check (identity in ('USER', 'SUPER'))
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
                where user_active.mid = _mid\s
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
end;
$$ language plpgsql;
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
				stmt.setInt(12, 0); // TODO: check default coins
				stmt.addBatch();
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user records, " + e.getMessage());
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
				danmuRecord.setId(++danmuCnt);
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

		// load user_watch_video: data not provided

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
				stmt.setLong(1, danmuRecord.getId());
				for (Long mid : danmuRecord.getLikedBy()) {
					stmt.setLong(2, mid);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert user_like_danmu records, " + e.getMessage());
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

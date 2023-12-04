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
    mid bigserial not null,
    name text not null,
    sex varchar(10),
    birthday date,
    level smallint not null default 0,
    sign text,
    identity varchar(10) not null,
    pwd char(256), -- encrypted by SHA256
    qqid varchar(20),
    wxid varchar(50),
    coin int default 0,
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
    constraint star_fk foreign key (star_mid) references user_info(mid),
    constraint fan_fk foreign key (fan_mid) references user_info(mid)
);

create table user_watch_video (
    mid bigint not null,
    bv varchar(15) not null,
    lastpos float8 not null, -- last watch time stamp in seconds
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
	values (?, ?, ?, to_date(?, 'YYYY-MM-DD'), ?, ?, ?, ?, ?, ?, ?);
		""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertUserInfoSQL)) {
			for (UserRecord userRecord : userRecords) {
				stmt.setLong(1, userRecord.getMid());
				stmt.setString(2, userRecord.getName());
				String sexString = userRecord.getSex();
				if (sexString.equals("M") || sexString.equals("男") || sexString.equals("♂")) {
					sexString = "MALE";
				}
				else if (sexString.equals("F") || sexString.equals("女") || sexString.equals("♀")) {
					sexString = "FEMALE";
				}
				else {
					sexString = "UNKNOWN";
				}
				stmt.setString(3, sexString);
				if (userRecord.getBirthday() != null) {
					String birthday = userRecord.getBirthday();
					int month = Integer.parseInt(birthday.substring(0, birthday.indexOf('月')));
					int day = Integer.parseInt(birthday.substring(birthday.indexOf('月') + 1, birthday.indexOf('日')));
					stmt.setString(4, String.format("1970-%02d-%02d", month, day));
				}
				else {
					stmt.setString(4, null);
				}
				stmt.setShort(5, userRecord.getLevel());
				stmt.setString(6, userRecord.getSign());
				stmt.setString(7, userRecord.getIdentity().name());
				stmt.setString(8, EncryptSHA256.encrypt(userRecord.getPassword()));
				stmt.setString(9, userRecord.getQq());
				stmt.setString(10, userRecord.getWechat());
				stmt.setDouble(11, 100.0); // TODO: check default coins
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
insert into danmu_info (bv, senderMid, showtime, content, posttime)
	values (?, ?, ?, ?, ?);
	""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(insertDanmuInfoSQL)) {
			for (DanmuRecord danmuRecord : danmuRecords) {
				stmt.setString(1, danmuRecord.getBv());
				stmt.setLong(2, danmuRecord.getMid());
				stmt.setFloat(3, danmuRecord.getTime());
				stmt.setString(4, danmuRecord.getContent());
				stmt.setTimestamp(5, danmuRecord.getPostTime());
				stmt.addBatch();
			}
		} catch (SQLException e) {
			System.out.println("[ERROR] Fail to insert danmu records, " + e.getMessage());
		}
		String getDanmuIdSQL = """
select danmu_id from danmu_info where bv = ? and senderMid = ? and showtime = ? and content = ? and posttime = ?;
			""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getDanmuIdSQL)) {
			for (DanmuRecord danmuRecord : danmuRecords) {
				stmt.setString(1, danmuRecord.getBv());
				stmt.setLong(2, danmuRecord.getMid());
				stmt.setFloat(3, danmuRecord.getTime());
				stmt.setString(4, danmuRecord.getContent());
				stmt.setTimestamp(5, danmuRecord.getPostTime());
				stmt.addBatch();
			}
			ResultSet rs = stmt.executeQuery();
			for (DanmuRecord danmuRecord : danmuRecords) {
				rs.next();
				danmuRecord.setId(rs.getLong(1));
			}
		}
		catch (SQLException e) {
			System.out.println("[ERROR] Fail to get danmu_id, " + e.getMessage());
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

		// TODO: load user_watch_video (not sure about the requirement)

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
